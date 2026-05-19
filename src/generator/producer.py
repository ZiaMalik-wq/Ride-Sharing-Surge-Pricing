import os
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
from confluent_kafka import Producer

# Ensure the repository root is importable when running this file directly.
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.shared.schemas import RideEvent
from src.shared.config import config

# Configuration
KAFKA_BOOTSTRAP_SERVERS = config.KAFKA_BOOTSTRAP_SERVERS
TOPIC_RIDE_REQUESTS = config.TOPIC_RIDE_REQUESTS
TOPIC_DRIVER_UPDATES = config.TOPIC_DRIVER_UPDATES
DATA_DIR = str(ROOT_DIR / 'data')

# Backpressure: pause briefly every N records to avoid overwhelming Flink's
# watermark tracking.  Set to 0 to disable throttling entirely.
BATCH_SIZE = 5000
BATCH_THROTTLE_SECONDS = float(os.getenv('PRODUCER_THROTTLE_SECONDS', '0.05'))

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'surge-pricing-nyc-producer',
    'queue.buffering.max.messages': 1000000  # Increase buffer for high throughput
}

producer = Producer(conf)


def delivery_report(err, msg):
    """Kafka delivery callback – logs failures so they aren't silently lost."""
    if err is not None:
        print(f'Message delivery failed: {err}')


def stream_real_data():
    if not os.path.exists(DATA_DIR):
        print(f"Error: Data directory '{DATA_DIR}' not found. Please run download script first.")
        return

    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.parquet')])
    if not files:
        print("Error: No parquet files found in data/ directory.")
        return

    print(f"Starting real-time simulation using {len(files)} dataset files...")
    if BATCH_THROTTLE_SECONDS > 0:
        print(f"Throttle: {BATCH_THROTTLE_SECONDS}s pause every {BATCH_SIZE} records")

    for file_name in files:
        file_path = os.path.join(DATA_DIR, file_name)
        print(f"\nStreaming records from: {file_name}")

        # Use PyArrow's iter_batches for memory-safe chunked reading instead
        # of materialising the entire Parquet file into a Python list of dicts.
        # Each batch is a small, bounded slice of the dataset (~50 000 rows).
        columns = [
            'request_datetime',
            'dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'hvfhs_license_num',
        ]

        try:
            parquet_file = pq.ParquetFile(file_path)
        except Exception as e:
            print(f"Error reading {file_name}: {e}")
            continue

        count = 0
        for batch in parquet_file.iter_batches(batch_size=50_000, columns=columns):
            # Convert each Arrow batch to a Pandas DataFrame for row iteration
            chunk = batch.to_pandas().sort_values('request_datetime')

            for row in chunk.itertuples(index=False):
                # 1. Map Pickup to Ride Request (Demand)
                request_ts = row.request_datetime
                ride_request = RideEvent.ride_request(
                    timestamp=request_ts,
                    zone_id=str(row.PULocationID),
                    user_id=str(row.hvfhs_license_num),
                )
                producer.produce(
                    TOPIC_RIDE_REQUESTS,
                    key=ride_request.zone_id,
                    value=ride_request.to_json(),
                    callback=delivery_report,
                )

                # 2. Map Dropoff to Driver Availability (Supply)
                dropoff_ts = row.dropoff_datetime
                driver_update = RideEvent.driver_available(
                    timestamp=dropoff_ts,
                    zone_id=str(row.DOLocationID),
                    driver_id=f"driver_{row.hvfhs_license_num}",
                )
                producer.produce(
                    TOPIC_DRIVER_UPDATES,
                    key=driver_update.zone_id,
                    value=driver_update.to_json(),
                    callback=delivery_report,
                )

                # Optimized polling + backpressure
                count += 1
                if count % 1000 == 0:
                    producer.poll(0)
                    if count % 10000 == 0:
                        print(f"Sent {count * 2} events... Latest Event Time: {request_ts}")

                # Throttle: give Flink time to keep up with watermarks
                if BATCH_THROTTLE_SECONDS > 0 and count % BATCH_SIZE == 0:
                    time.sleep(BATCH_THROTTLE_SECONDS)

        producer.flush()
        print(f"\nFinished streaming {file_name} ({count * 2} total events)")


if __name__ == '__main__':
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    try:
        stream_real_data()
    except KeyboardInterrupt:
        print("\nStopping simulation...")
    finally:
        producer.flush()
        print("Done.")
