CREATE KEYSPACE IF NOT EXISTS mission_telemetry
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE mission_telemetry;

CREATE TABLE IF NOT EXISTS telemetry_by_sensor_day (
    satellite_id text,
    sensor_id text,
    day_bucket date,
    observed_at timestamp,
    event_id uuid,
    metric text,
    value double,
    unit text,
    module_id text,
    sequence bigint,
    PRIMARY KEY ((satellite_id, sensor_id, day_bucket), observed_at, event_id)
) WITH CLUSTERING ORDER BY (observed_at DESC, event_id ASC);
