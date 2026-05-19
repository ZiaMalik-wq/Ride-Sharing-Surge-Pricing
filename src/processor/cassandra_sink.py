import json
import logging
import sys
import time
from pathlib import Path

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel
from confluent_kafka import Consumer, KafkaError

# Ensure the repository root is importable when running this file directly.
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.shared.schemas import SurgeWindowRecord
from src.shared.config import config

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = config.KAFKA_BOOTSTRAP_SERVERS
CASSANDRA_HOST = config.CASSANDRA_HOST
CASSANDRA_KEYSPACE = config.CASSANDRA_KEYSPACE
KAFKA_TOPIC = config.TOPIC_SURGE_PRICING

# Connection / batching parameters
MAX_CONNECT_RETRIES = 5
CONNECT_RETRY_DELAY = 5
BATCH_FLUSH_SIZE = 50


class CassandraSink:
    """Consumes surge pricing records from Kafka and archives them to Cassandra.

    Key design decisions:
    - ``enable.auto.commit`` is **disabled** so offsets are only committed after
      a successful Cassandra write — giving at-least-once delivery semantics.
    - Writes are accumulated into a ``BatchStatement`` and flushed every
      ``BATCH_FLUSH_SIZE`` records for higher throughput.
    - The Cassandra schema is auto-detected at startup (legacy vs. windowed)
      to stay backwards-compatible.
    """

    def __init__(
        self,
        kafka_broker: str = KAFKA_BROKER,
        cassandra_host: str = CASSANDRA_HOST,
        keyspace: str = CASSANDRA_KEYSPACE,
        topic: str = KAFKA_TOPIC,
    ) -> None:
        self.topic = topic
        self.keyspace = keyspace

        # Cassandra connection with retries
        self.cluster, self.session = self._connect_cassandra(cassandra_host)
        self.insert_stmt, self.use_legacy_schema = self._detect_schema()

        # Kafka consumer with manual offset management
        conf = {
            "bootstrap.servers": kafka_broker,
            "group.id": "cassandra-sink-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([self.topic])

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _connect_cassandra(self, host: str) -> tuple:
        """Connect to Cassandra with bounded retries."""
        for attempt in range(1, MAX_CONNECT_RETRIES + 1):
            try:
                cluster = Cluster([host])
                session = cluster.connect(self.keyspace)
                logger.info("Connected to Cassandra keyspace: %s", self.keyspace)
                return cluster, session
            except Exception as exc:
                logger.warning(
                    "Connection attempt %d/%d failed: %s. Retrying in %ds...",
                    attempt,
                    MAX_CONNECT_RETRIES,
                    exc,
                    CONNECT_RETRY_DELAY,
                )
                if attempt < MAX_CONNECT_RETRIES:
                    time.sleep(CONNECT_RETRY_DELAY)

        logger.error(
            "Could not connect to Cassandra after %d attempts. Exiting.",
            MAX_CONNECT_RETRIES,
        )
        sys.exit(1)

    def _detect_schema(self) -> tuple:
        """Auto-detect the historical_surge table schema and prepare the
        appropriate INSERT statement."""
        rows = self.session.execute(
            """
            SELECT column_name
            FROM system_schema.columns
            WHERE keyspace_name = %s AND table_name = %s
            """,
            (self.keyspace, "historical_surge"),
        )
        existing_columns = {row.column_name for row in rows}

        if "window_end" in existing_columns:
            logger.info("Using Cassandra historical schema with window_end.")
            stmt = self.session.prepare(
                """
                INSERT INTO historical_surge (
                    zone_id, window_end, window_start,
                    demand, supply, surge_multiplier, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            )
            return stmt, False

        logger.warning(
            "Using legacy Cassandra schema with ts. "
            "Consider rerunning init_cassandra.cql for the newer windowed schema."
        )
        stmt = self.session.prepare(
            """
            INSERT INTO historical_surge (
                zone_id, ts, surge_multiplier
            ) VALUES (?, ?, ?)
            """
        )
        return stmt, True

    # ------------------------------------------------------------------
    # Batched writes
    # ------------------------------------------------------------------

    def _bind_record(self, record: SurgeWindowRecord):
        """Return a bound statement for a single record."""
        if self.use_legacy_schema:
            return self.insert_stmt.bind((
                record.zone_id,
                record.window_end,
                float(record.surge_multiplier),
            ))

        return self.insert_stmt.bind((
            record.zone_id,
            record.window_end,
            record.window_start,
            int(record.demand),
            int(record.supply),
            float(record.surge_multiplier),
            record.updated_at or record.window_end,
        ))

    def _flush_batch(self, batch: BatchStatement, records: list, messages: list) -> None:
        """Execute the accumulated batch and commit Kafka offsets."""
        if not records:
            return

        try:
            self.session.execute(batch)
            # Commit the offset of the *last* message in the batch
            self.consumer.commit(messages[-1])
            for rec in records:
                logger.info(
                    "Archived: Zone %s @ %s -> %fx",
                    rec.zone_id,
                    rec.window_end.isoformat(),
                    rec.surge_multiplier,
                )
        except Exception as exc:
            logger.error("Batch write failed (%d records): %s", len(records), exc)

    # ------------------------------------------------------------------
    # Main consume loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the consume → batch → write → commit loop."""
        logger.info("Started Cassandra Sink Service. Listening to '%s' topic...", self.topic)

        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        pending_records: list[SurgeWindowRecord] = []
        pending_messages = []

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # Flush any partial batch on idle
                    self._flush_batch(batch, pending_records, pending_messages)
                    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                    pending_records.clear()
                    pending_messages.clear()
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error: %s", msg.error())
                    continue

                # upsert-kafka sends a null value when a row is deleted/retracted
                if msg.value() is None:
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    record = SurgeWindowRecord.from_dict(payload)
                    batch.add(self._bind_record(record))
                    pending_records.append(record)
                    pending_messages.append(msg)

                    # Flush when the batch reaches the configured size
                    if len(pending_records) >= BATCH_FLUSH_SIZE:
                        self._flush_batch(batch, pending_records, pending_messages)
                        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                        pending_records.clear()
                        pending_messages.clear()

                except Exception as exc:
                    logger.error("Error processing message: %s", exc)

        except KeyboardInterrupt:
            logger.info("Stopping Cassandra Sink...")
            # Flush remaining records before shutdown
            self._flush_batch(batch, pending_records, pending_messages)
        finally:
            self.consumer.close()
            self.cluster.shutdown()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Shutdown consumer and Cassandra connections."""
        self.consumer.close()
        self.cluster.shutdown()


def main() -> None:
    try:
        sink = CassandraSink()
        sink.run()
    except Exception as exc:
        logger.error("Fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
