import json
import redis
from confluent_kafka import Consumer, KafkaError

# Configuration
KAFKA_BROKER = 'localhost:9092'  # Assuming you run this on your Windows host
REDIS_HOST = 'localhost'

# Connect to Redis
try:
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    r.ping()
except Exception as e:
    print(f"Could not connect to Redis: {e}")
    exit(1)

# Kafka Consumer for Surge Output
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'surge-redis-sink-group',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(conf)
consumer.subscribe(['surge_pricing'])

print("🗄️ Started Redis Sink Service.")
print("Listening to 'surge_pricing' Kafka topic...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Kafka error: {msg.error()}")
            continue

        # upsert-kafka sends a null value when a row is deleted/retracted
        if msg.value() is None:
            continue
            
        try:
            # Parse Flink's output
            data = json.loads(msg.value().decode('utf-8'))
            zone_id = data.get('zone_id')
            
            if zone_id:
                # Store in Redis
                r.set(f"surge:{zone_id}", json.dumps(data))
                print(f"📦 Saved to Redis: surge:{zone_id} -> {data['surge_multiplier']}x")
        except Exception as e:
            print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print("\nStopping Redis Sink...")
finally:
    consumer.close()
