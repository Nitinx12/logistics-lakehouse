-- Audit tables for extract jobs: one row per collection load.
-- Extractors fall back to this shape when sql/01_source_etl_logs.sql is absent.
BEGIN;

CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE IF NOT EXISTS source.etl_logs (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT,
    job_name VARCHAR NOT NULL,
    collection_name VARCHAR NOT NULL,
    target_schema VARCHAR NOT NULL,
    target_table VARCHAR NOT NULL,
    mode VARCHAR NOT NULL,
    watermark_column VARCHAR NOT NULL,
    watermark_from TIMESTAMPTZ,
    watermark_to TIMESTAMPTZ,
    rows_extracted BIGINT NOT NULL DEFAULT 0,
    rows_loaded BIGINT NOT NULL DEFAULT 0,
    rows_inserted BIGINT NOT NULL DEFAULT 0,
    rows_updated BIGINT NOT NULL DEFAULT 0,
    chunks INTEGER NOT NULL DEFAULT 0,
    status VARCHAR NOT NULL,
    validation_status VARCHAR NOT NULL DEFAULT 'N/A',
    validation_detail TEXT,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

GRANT USAGE ON SCHEMA source TO etl_writer, dq_runner;
GRANT ALL ON source.etl_logs TO etl_writer;
GRANT SELECT ON source.etl_logs TO dq_runner;

COMMIT;
