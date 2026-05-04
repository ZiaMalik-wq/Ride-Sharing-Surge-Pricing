import os
import json
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

def run_surge_processor():
    # 1. Setup Table Environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    # 2. Add Kafka Connector JAR
    # Since we are running in Docker, we point to the volume-mapped lib folder
    # or download it if it's not there.
    jar_path = "/opt/flink/project/lib/flink-sql-connector-kafka-3.0.1-1.18.jar"
    t_env.get_config().set("pipeline.jars", f"file://{jar_path}")

    # 3. Define Kafka Source: Ride Requests
    # Note: Using 'kafka:29092' for internal Docker communication
    t_env.execute_sql("""
        CREATE TABLE ride_requests (
            timestamp_str STRING,
            zone_id STRING,
            event_type STRING,
            user_id STRING,
            ts AS TO_TIMESTAMP(REPLACE(timestamp_str, 'T', ' ')),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'ride_requests',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-surge-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # 4. Define Kafka Source: Driver Updates
    t_env.execute_sql("""
        CREATE TABLE driver_updates (
            timestamp_str STRING,
            zone_id STRING,
            event_type STRING,
            driver_id STRING,
            ts AS TO_TIMESTAMP(REPLACE(timestamp_str, 'T', ' ')),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'driver_updates',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-surge-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # 5. Define Windowed Aggregations
    demand_table = t_env.sql_query("""
        SELECT 
            zone_id, 
            COUNT(user_id) as demand,
            HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_end
        FROM ride_requests
        GROUP BY zone_id, HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)
    """)
    t_env.create_temporary_view("demand_view", demand_table)

    supply_table = t_env.sql_query("""
        SELECT 
            zone_id, 
            COUNT(driver_id) as supply,
            HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_end
        FROM driver_updates
        GROUP BY zone_id, HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)
    """)
    t_env.create_temporary_view("supply_view", supply_table)

    # 6. Compute Surge Multiplier
    surge_table = t_env.sql_query("""
        SELECT 
            d.zone_id,
            d.window_end as ts,
            CAST(GREATEST(1.0, CAST(d.demand AS DOUBLE) / CAST((COALESCE(s.supply, 0) + 1) AS DOUBLE)) AS DOUBLE) as surge_multiplier
        FROM demand_view d
        LEFT JOIN supply_view s ON d.zone_id = s.zone_id AND d.window_end = s.window_end
    """)

    # 7. Output Result
    # For Phase 3/4, we'll use print() to verify Flink is working.
    # In Phase 4, we will connect this to a formal Redis Sink.
    print("Submitting Surge Pricing Flink Job to Docker Cluster...")
    surge_table.execute().print()

if __name__ == '__main__':
    run_surge_processor()
