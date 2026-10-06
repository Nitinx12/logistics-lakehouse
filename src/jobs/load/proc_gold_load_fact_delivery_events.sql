/*
================================================================================
Procedure : gold.load_fact_delivery_events
Purpose   : Incrementally upsert one row per delivery event with keys.

Source    : silver.delivery_events
Target    : gold.fact_delivery_events
Grain     : One row per event_id

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated silver rows using the loaded_at watermark.
5. Resolve facility keys, keeping degenerate load and trip ids.
6. Upsert rows on the grain key, keeping the newer loaded_at row.
7. Record rows_in, rows_out, execution status, and errors.
8. Commit the transaction and release the advisory lock.

Error Handling
--------------
- Captures PostgreSQL error message and SQLSTATE.
- Marks the ETL run as FAILED.
- Commits the failure audit record.
- Releases the advisory lock.
- Re-raises the original error to the caller.

Parameters
----------
p_batch_id : TEXT
    Unique identifier for the current ETL batch.

Returns
-------
None

Concurrency
-----------
Uses a PostgreSQL advisory lock to ensure only one instance of this
procedure runs at a time.

Load Strategy
-------------
Incremental upsert on the grain key driven by the loaded_at watermark.
Rerunning a batch yields the same state.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS gold.fact_delivery_events (
    event_id VARCHAR PRIMARY KEY,
    load_id VARCHAR NOT NULL,
    trip_id VARCHAR NOT NULL,
    facility_sk BIGINT NOT NULL REFERENCES gold.dim_facility (facility_sk),
    event_type VARCHAR NOT NULL,
    scheduled_datetime TIMESTAMP NOT NULL,
    actual_datetime TIMESTAMP NOT NULL,
    delay_minutes INTEGER NOT NULL,
    detention_minutes INTEGER NOT NULL,
    on_time_flag BOOLEAN NOT NULL,
    location_city VARCHAR NOT NULL,
    location_state VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    gold_batch_id TEXT,
    gold_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    gold_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_delivery_events_trip_id
ON gold.fact_delivery_events (trip_id);

CREATE INDEX IF NOT EXISTS idx_fact_delivery_events_load_id
ON gold.fact_delivery_events (load_id);

CREATE OR REPLACE PROCEDURE gold.load_fact_delivery_events(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'gold.load_fact_delivery_events';
    c_target CONSTANT VARCHAR := 'gold.fact_delivery_events';
    c_lock_key CONSTANT BIGINT := hashtextextended('gold.load_fact_delivery_events', 0);
    v_log_id BIGINT;
    v_rows_in BIGINT := 0;
    v_rows_out BIGINT := 0;
    v_error TEXT;
    v_sqlstate TEXT;
BEGIN
    IF NOT pg_try_advisory_lock(c_lock_key) THEN
        RAISE EXCEPTION '% is already running', c_procedure
            USING ERRCODE = '55P03';
    END IF;

    UPDATE gold.etl_logs
    SET
        status = 'FAILED',
        error_message = 'Orphaned run: session ended before completion',
        finished_at = NOW()
    WHERE procedure_name = c_procedure
        AND target_table = c_target
        AND status = 'STARTED';

    INSERT INTO gold.etl_logs (
        procedure_name,
        batch_id,
        target_table
    )
    VALUES (
        c_procedure,
        p_batch_id,
        c_target
    )
    RETURNING id INTO v_log_id;

    COMMIT;

    BEGIN
        SET LOCAL lock_timeout = '30s';

        CREATE TEMP TABLE stg_fact_events ON COMMIT DROP AS
        SELECT
            s.event_id,
            s.load_id,
            s.trip_id,
            COALESCE(df.facility_sk, -1) AS facility_sk,
            s.event_type,
            s.scheduled_datetime,
            s.actual_datetime,
            s.delay_minutes,
            s.detention_minutes,
            s.on_time_flag,
            s.location_city,
            s.location_state,
            s.loaded_at
        FROM silver.delivery_events AS s
        LEFT JOIN gold.dim_facility AS df
            ON df.facility_id = s.facility_id
        WHERE s.loaded_at >= COALESCE(
            (SELECT MAX(f.loaded_at) FROM gold.fact_delivery_events AS f),
            '-infinity'::TIMESTAMPTZ
        );

        SELECT COUNT(*)
        INTO v_rows_in
        FROM stg_fact_events;

        INSERT INTO gold.fact_delivery_events AS t (
            event_id,
            load_id,
            trip_id,
            facility_sk,
            event_type,
            scheduled_datetime,
            actual_datetime,
            delay_minutes,
            detention_minutes,
            on_time_flag,
            location_city,
            location_state,
            loaded_at,
            gold_batch_id
        )
        SELECT
            event_id,
            load_id,
            trip_id,
            facility_sk,
            event_type,
            scheduled_datetime,
            actual_datetime,
            delay_minutes,
            detention_minutes,
            on_time_flag,
            location_city,
            location_state,
            loaded_at,
            p_batch_id
        FROM stg_fact_events
        ON CONFLICT (event_id) DO UPDATE
        SET
            load_id = EXCLUDED.load_id,
            trip_id = EXCLUDED.trip_id,
            facility_sk = EXCLUDED.facility_sk,
            event_type = EXCLUDED.event_type,
            scheduled_datetime = EXCLUDED.scheduled_datetime,
            actual_datetime = EXCLUDED.actual_datetime,
            delay_minutes = EXCLUDED.delay_minutes,
            detention_minutes = EXCLUDED.detention_minutes,
            on_time_flag = EXCLUDED.on_time_flag,
            location_city = EXCLUDED.location_city,
            location_state = EXCLUDED.location_state,
            loaded_at = EXCLUDED.loaded_at,
            gold_batch_id = EXCLUDED.gold_batch_id,
            gold_updated_at = NOW()
        WHERE t.loaded_at < EXCLUDED.loaded_at;

        GET DIAGNOSTICS v_rows_out = ROW_COUNT;

        UPDATE gold.etl_logs
        SET
            status = 'SUCCESS',
            rows_in = v_rows_in,
            rows_out = v_rows_out,
            finished_at = NOW()
        WHERE id = v_log_id;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error = MESSAGE_TEXT,
                v_sqlstate = RETURNED_SQLSTATE;
    END;

    IF v_error IS NOT NULL THEN
        UPDATE gold.etl_logs
        SET
            status = 'FAILED',
            rows_in = v_rows_in,
            error_message = v_error,
            finished_at = NOW()
        WHERE id = v_log_id;

        COMMIT;

        PERFORM pg_advisory_unlock(c_lock_key);

        RAISE EXCEPTION '% failed for batch %: %', c_procedure, p_batch_id, v_error
            USING ERRCODE = v_sqlstate;
    END IF;

    COMMIT;

    PERFORM pg_advisory_unlock(c_lock_key);
END;
$$;

GRANT ALL ON gold.fact_delivery_events TO etl_writer;
GRANT SELECT ON gold.fact_delivery_events TO dq_runner;
GRANT SELECT ON gold.fact_delivery_events TO analyst_ro;
GRANT SELECT ON gold.fact_delivery_events TO dashboard_ro;
GRANT USAGE, SELECT ON SEQUENCE gold.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE gold.load_fact_delivery_events(TEXT) TO etl_writer;

COMMIT;
