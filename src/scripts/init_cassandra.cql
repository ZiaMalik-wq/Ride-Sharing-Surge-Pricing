-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS surge_analytics 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Create Table for Historical Surge Multipliers
-- Partitioned by zone_id, clustered by timestamp (descending)
CREATE TABLE IF NOT EXISTS surge_analytics.historical_surge (
    zone_id text,
    ts timestamp,
    surge_multiplier double,
    PRIMARY KEY (zone_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- Example query: Get the last 24 hours of surge pricing for a specific zone
-- SELECT * FROM surge_analytics.historical_surge WHERE zone_id = 'zone_1' AND ts > '2024-01-01 00:00:00';
