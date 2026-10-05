-- Run log and layer snapshot for src/utils/tracking.py.
BEGIN;

CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (
    run_id VARCHAR NOT NULL,
    stage VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'STARTED',
    attempt INTEGER NOT NULL DEFAULT 1,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_s NUMERIC,
    rows_in BIGINT,
    rows_out BIGINT,
    rows_rejected BIGINT,
    detail JSONB NOT NULL DEFAULT '{}'::JSONB,
    CONSTRAINT pk_pipeline_run_log
        PRIMARY KEY (run_id, stage),
    CONSTRAINT chk_pipeline_run_log_status
        CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED'))
);

CREATE TABLE IF NOT EXISTS ops.layer_snapshot (
    run_id VARCHAR NOT NULL,
    layer VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    rows_n BIGINT NOT NULL DEFAULT 0,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_layer_snapshot
        PRIMARY KEY (run_id, layer, table_name)
);

CREATE OR REPLACE PROCEDURE ops.record_layer_snapshot(p_run_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema TEXT;
    v_table TEXT;
    v_count BIGINT;
BEGIN
    DELETE FROM ops.layer_snapshot
    WHERE run_id = p_run_id;
    FOREACH v_schema IN ARRAY ARRAY['bronze', 'silver', 'gold', 'analytics']
    LOOP
        FOR v_table IN
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = v_schema
        LOOP
            EXECUTE format('SELECT COUNT(*) FROM %I.%I', v_schema, v_table)
            INTO v_count;
            INSERT INTO ops.layer_snapshot (run_id, layer, table_name, rows_n)
            VALUES (p_run_id, v_schema, v_table, v_count);
        END LOOP;
    END LOOP;
END;
$$;

GRANT ALL ON ops.pipeline_run_log TO etl_writer;
GRANT ALL ON ops.layer_snapshot TO etl_writer;
GRANT SELECT ON ops.pipeline_run_log TO dq_runner;
GRANT SELECT ON ops.layer_snapshot TO dq_runner;

COMMIT;
