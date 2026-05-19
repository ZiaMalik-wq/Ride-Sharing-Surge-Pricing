import json
import logging
import sys
import time
from pathlib import Path

import redis
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
REDIS_HOST = config.REDIS_HOST
REDIS_PORT = config.REDIS_PORT
KAFKA_TOPIC = config.TOPIC_SURGE_PRICING

# Retry parameters for transient Redis failures
MAX_REDIS_RETRIES = 3
REDIS_RETRY_DELAY_SECONDS = 1.0


class RedisSink:
    """Consumes surge pricing records from Kafka and persists them to Redis.

    Key design decisions:
    - ``enable.auto.commit`` is **disabled** so that offsets are only committed
      after a successful Redis write.  This gives at-least-once semantics:
      if the sink crashes mid-batch the message will be re-delivered on restart.
    - Transient Redis failures are retried a bounded number of times before the
      message is logged and skipped (to prevent infinite loops).
    """

    def __init__(
        self,
        kafka_broker: str = KAFKA_BROKER,
        redis_host: str = REDIS_HOST,
        redis_port: int = REDIS_PORT,
        topic: str = KAFKA_TOPIC,
    ) -> None:
        self.topic = topic

        # Redis connection
        self.redis = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True
        )
        self._assert_redis_connection()

        # Kafka consumer with manual offset management
        conf = {
            "bootstrap.servers": kafka_broker,
            "group.id": "surge-redis-sink-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([self.topic])

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _assert_redis_connection(self) -> None:
        """Verify Redis is reachable; raise on failure."""
        try:
            self.redis.ping()
        except Exception as exc:
            logger.error("Could not connect to Redis: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Redis write with retry
    # ------------------------------------------------------------------

    def _write_to_redis(self, record: SurgeWindowRecord) -> bool:
        """Persist a single surge record to Redis, retrying on transient errors.

        Returns ``True`` if the write succeeded, ``False`` otherwise.
        """
        zone_id = str(record.zone_id)

        for attempt in range(1, MAX_REDIS_RETRIES + 1):
            try:
                pipe = self.redis.pipeline()
                pipe.set(f"surge:{zone_id}", record.to_json())
                pipe.sadd("surge:zones", zone_id)
                pipe.set(
                    "surge:latest_update",
                    (record.updated_at or record.window_end).isoformat(),
                )
                pipe.execute()
                return True
            except redis.ConnectionError as exc:
                logger.warning(
                    "Redis write failed (attempt %d/%d): %s",
                    attempt,
                    MAX_REDIS_RETRIES,
                    exc,
                )
                if attempt < MAX_REDIS_RETRIES:
                    time.sleep(REDIS_RETRY_DELAY_SECONDS)
            except Exception as exc:
                logger.error("Unexpected Redis error: %s", exc)
                return False

        logger.error(
            "Redis write failed after %d retries for zone %s — skipping message",
            MAX_REDIS_RETRIES,
            zone_id,
        )
        return False

    # ------------------------------------------------------------------
    # Main consume loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the consume → write → commit loop."""
        logger.info("🗄️  Started Redis Sink Service.")
        logger.info("Listening to '%s' Kafka topic...", self.topic)

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
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

                    if not record.zone_id:
                        logger.warning(
                            "Received record with missing zone_id: %s", payload
                        )
                        continue

                    if self._write_to_redis(record):
                        # Only commit the offset after a confirmed write
                        self.consumer.commit(msg)
                        logger.info(
                            "✅ Live Update | Zone: %-5s | Surge: %sx | D: %s | S: %s",
                            record.zone_id,
                            record.surge_multiplier,
                            record.demand,
                            record.supply,
                        )
                except Exception as exc:
                    logger.error("Error processing message: %s", exc)
                    if msg.value():
                        logger.debug("Raw Value: %s", msg.value().decode("utf-8"))

        except KeyboardInterrupt:
            logger.info("Stopping Redis Sink...")
        finally:
            self.consumer.close()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Shutdown consumer and Redis connections."""
        self.consumer.close()
        self.redis.close()


def main() -> None:
    try:
        sink = RedisSink()
        sink.run()
    except Exception as exc:
        logger.error("Fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
