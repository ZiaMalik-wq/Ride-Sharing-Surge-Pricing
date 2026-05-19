import sys
from pathlib import Path
from pyflink.table import EnvironmentSettings, TableEnvironment

# Ensure the repository root is importable when running inside Flink
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.shared.config import config

def run_surge_processor():
    # 1. Setup Environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    # 2. Add Kafka Connector JAR and Restart Strategy
    jar_path = "/opt/flink/project/lib/flink-sql-connector-kafka-3.0.1-1.18.jar"
    t_env.get_config().set("pipeline.jars", f"file://{jar_path}")
    
    # Configure Restart Strategy: 10 attempts, 10s delay
    t_env.get_config().set("restart-strategy.type", "fixed-delay")
    t_env.get_config().set("restart-strategy.fixed-delay.attempts", "10")
    t_env.get_config().set("restart-strategy.fixed-delay.delay", "10 s")

    # Enable checkpointing for fault-tolerant exactly-once processing.
    # Without checkpoints a Flink failure causes full replay from earliest-offset.
    t_env.get_config().set("execution.checkpointing.interval", "60000")
    t_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")

    # 3. Define Kafka Source: Ride Requests
    t_env.execute_sql("""
        CREATE TABLE ride_requests (
            `timestamp` STRING,
            zone_id STRING,
            event_type STRING,
            user_id STRING,
            -- Truncate to 19 chars to handle varying sub-second precision
            ts AS TO_TIMESTAMP(SUBSTR(REPLACE(`timestamp`, 'T', ' '), 1, 19)),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.TOPIC_RIDE_REQUESTS}',
            'properties.bootstrap.servers' = '{config.KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-ride-requests-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """.format(config=config))

    # 4. Define Kafka Source: Driver Updates
    t_env.execute_sql("""
        CREATE TABLE driver_updates (
            `timestamp` STRING,
            zone_id STRING,
            event_type STRING,
            driver_id STRING,
            -- Truncate to 19 chars for robustness
            ts AS TO_TIMESTAMP(SUBSTR(REPLACE(`timestamp`, 'T', ' '), 1, 19)),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.TOPIC_DRIVER_UPDATES}',
            'properties.bootstrap.servers' = '{config.KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-driver-updates-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """.format(config=config))

    # 5. Build a unified event stream and compute demand/supply in one hop window.
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW ride_events AS
        SELECT
            zone_id,
            event_type,
            ts
        FROM ride_requests

        UNION ALL

        SELECT
            zone_id,
            event_type,
            ts
        FROM driver_updates
    """)


    # 6. Define Kafka Sink for Output.
    # We use 'kafka' connector for a simpler, flat JSON stream.
    t_env.execute_sql("""
        CREATE TABLE surge_output (
            zone_id STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            demand BIGINT,
            supply BIGINT,
            surge_multiplier DOUBLE,
            updated_at TIMESTAMP_LTZ(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.TOPIC_SURGE_PRICING}',
            'properties.bootstrap.servers' = '{config.KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json'
        )
    """.format(config=config))

    # 7. Insert processed data into Output Topic
    # We use a subquery to ensure demand/supply are calculated once and reused for the multiplier.
    print("Starting Flink Job: Sinking Surge Multipliers to Kafka...")
    t_env.execute_sql("""
        INSERT INTO surge_output
        SELECT 
            zone_id, 
            window_start, 
            window_end, 
            demand, 
            supply, 
            CAST(1.0 + (CAST(demand AS DOUBLE) / CAST(supply + 1 AS DOUBLE)) AS DOUBLE) as surge_multiplier,
            CURRENT_TIMESTAMP as updated_at
        FROM (
            SELECT 
                zone_id, 
                HOP_START(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_start,
                HOP_END(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_end,
                SUM(CASE WHEN event_type = 'ride_request' THEN 1 ELSE 0 END) AS demand,
                SUM(CASE WHEN event_type = 'driver_available' THEN 1 ELSE 0 END) AS supply
            FROM ride_events
            GROUP BY zone_id, HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
        )
    """)

if __name__ == '__main__':
    run_surge_processor()
