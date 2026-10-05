-- Audit table for incremental loads: one row per table load
-- Run with psql: psql -U postgres -f 01_bronze_etl_logs.sql
BEGIN;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.etl_logs (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR NOT NULL,
    target_table VARCHAR NOT NULL,
    watermark_from TIMESTAMPTZ,
    watermark_to TIMESTAMPTZ,
    rows_extracted BIGINT NOT NULL DEFAULT 0,
    rows_loaded BIGINT NOT NULL DEFAULT 0,
    status VARCHAR NOT NULL DEFAULT 'STARTED',
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT chk_etl_logs_status
        CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_etl_logs_last_success
    ON bronze.etl_logs (target_table, watermark_to DESC)
    WHERE status = 'SUCCESS';

CREATE UNIQUE INDEX IF NOT EXISTS uq_etl_logs_single_started_run
    ON bronze.etl_logs (job_name, target_table)
    WHERE status = 'STARTED';

COMMIT;
