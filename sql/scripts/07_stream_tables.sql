-- Stream sink landing table; the sink only writes, never creates.
-- Run with psql: psql -U postgres -f 07_stream_tables.sql
BEGIN;

CREATE TABLE IF NOT EXISTS bronze.delivery_events_stream (
    event_id VARCHAR PRIMARY KEY,
    trip_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    batch_id VARCHAR NOT NULL DEFAULT 'stream'
);

GRANT ALL ON bronze.delivery_events_stream TO etl_writer;
GRANT SELECT ON bronze.delivery_events_stream TO dq_runner;

COMMIT;
