-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS surge_analytics 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Create Table for Historical Surge Multipliers
-- Partitioned by zone_id, clustered by window_end (descending)
CREATE TABLE IF NOT EXISTS surge_analytics.historical_surge (
    zone_id text,
    window_start timestamp,
    window_end timestamp,
    demand bigint,
    supply bigint,
    surge_multiplier double,
    updated_at timestamp,
    PRIMARY KEY ((zone_id), window_end)
) WITH CLUSTERING ORDER BY (window_end DESC);

-- Example query: Get the last 24 hours of surge pricing for a specific zone
-- SELECT * FROM surge_analytics.historical_surge WHERE zone_id = 'zone_1' AND window_end > '2024-01-01 00:00:00';
