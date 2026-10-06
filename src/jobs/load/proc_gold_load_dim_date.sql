/*
================================================================================
Procedure : gold.load_dim_date
Purpose   : Fully rebuild the calendar dimension covering all business dates,
            including the unknown member.

Source    : Generated calendar series
Target    : gold.dim_date

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Truncate and rebuild every calendar day from 2010 to 2030.
5. Ensure the unknown member row (date_key = -1) exists.
6. Record rows_in, rows_out, execution status, and errors.
7. Commit the transaction and release the advisory lock.

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
Incremental insert of missing calendar days. Dates never change, so new
days insert once and reruns change nothing. Rerunning yields the same state.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year_number INTEGER NOT NULL,
    quarter_number INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name VARCHAR NOT NULL,
    day_number INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    gold_batch_id TEXT,
    gold_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_date_full_date UNIQUE (full_date)
);

CREATE OR REPLACE PROCEDURE gold.load_dim_date(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'gold.load_dim_date';
    c_target CONSTANT VARCHAR := 'gold.dim_date';
    c_lock_key CONSTANT BIGINT := hashtextextended('gold.load_dim_date', 0);
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

        CREATE TEMP TABLE stg_dim_date ON COMMIT DROP AS
        SELECT
            TO_CHAR(day::DATE, 'YYYYMMDD')::INTEGER AS date_key,
            day::DATE AS full_date,
            EXTRACT(YEAR FROM day)::INTEGER AS year_number,
            EXTRACT(QUARTER FROM day)::INTEGER AS quarter_number,
            EXTRACT(MONTH FROM day)::INTEGER AS month_number,
            TO_CHAR(day::DATE, 'Month') AS month_name,
            EXTRACT(DAY FROM day)::INTEGER AS day_number,
            EXTRACT(ISODOW FROM day)::INTEGER AS day_of_week,
            TO_CHAR(day::DATE, 'Day') AS day_name,
            EXTRACT(ISODOW FROM day)::INTEGER IN (6, 7) AS is_weekend
        FROM GENERATE_SERIES(
            '2010-01-01'::TIMESTAMP,
            '2030-12-31'::TIMESTAMP,
            '1 day'::INTERVAL
        ) AS day;

        INSERT INTO gold.dim_date (
            date_key,
            full_date,
            year_number,
            quarter_number,
            month_number,
            month_name,
            day_number,
            day_of_week,
            day_name,
            is_weekend,
            gold_batch_id
        )
        SELECT
            date_key,
            full_date,
            year_number,
            quarter_number,
            month_number,
            month_name,
            day_number,
            day_of_week,
            day_name,
            is_weekend,
            p_batch_id
        FROM stg_dim_date
        ON CONFLICT (date_key) DO NOTHING;

        GET DIAGNOSTICS v_rows_out = ROW_COUNT;

        SELECT COUNT(*)
        INTO v_rows_in
        FROM stg_dim_date;

        INSERT INTO gold.dim_date (
            date_key,
            full_date,
            year_number,
            quarter_number,
            month_number,
            month_name,
            day_number,
            day_of_week,
            day_name,
            is_weekend,
            gold_batch_id
        )
        VALUES (
            -1,
            '1900-01-01',
            1900,
            1,
            1,
            'Unknown',
            1,
            1,
            'Unknown',
            FALSE,
            p_batch_id
        )
        ON CONFLICT (date_key) DO NOTHING;

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

GRANT ALL ON gold.dim_date TO etl_writer;
GRANT SELECT ON gold.dim_date TO dq_runner;
GRANT SELECT ON gold.dim_date TO analyst_ro;
GRANT SELECT ON gold.dim_date TO dashboard_ro;
GRANT USAGE, SELECT ON SEQUENCE gold.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE gold.load_dim_date(TEXT) TO etl_writer;

COMMIT;
