-- Audit table for silver loads: one row per procedure run.
-- Procedures are named silver.load_<table> and take p_batch_id TEXT.
BEGIN;

CREATE TABLE IF NOT EXISTS silver.etl_logs (
    id BIGSERIAL PRIMARY KEY,
    procedure_name VARCHAR NOT NULL,
    batch_id TEXT,
    target_table VARCHAR NOT NULL,
    rows_in BIGINT NOT NULL DEFAULT 0,
    rows_out BIGINT NOT NULL DEFAULT 0,
    rows_rejected BIGINT NOT NULL DEFAULT 0,
    status VARCHAR NOT NULL DEFAULT 'STARTED',
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT chk_silver_etl_logs_status
        CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_silver_etl_logs_last_success
    ON silver.etl_logs (target_table, finished_at DESC)
    WHERE status = 'SUCCESS';

CREATE UNIQUE INDEX IF NOT EXISTS uq_silver_etl_logs_single_started_run
    ON silver.etl_logs (procedure_name, target_table)
    WHERE status = 'STARTED';

GRANT ALL ON silver.etl_logs TO etl_writer;
GRANT SELECT ON silver.etl_logs TO dq_runner;

COMMIT;
