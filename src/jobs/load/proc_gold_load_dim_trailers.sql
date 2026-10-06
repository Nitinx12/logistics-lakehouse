/*
================================================================================
Procedure : gold.load_dim_trailers
Purpose   : Incrementally upsert trailer dimension rows from silver,
            keeping stable surrogate keys and the unknown member.

Source    : silver.trailers
Target    : gold.dim_trailer

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Ensure the unknown member row (trailer_sk = -1) exists.
5. Read new and updated silver rows using the loaded_at watermark.
6. Upsert rows on the business key, overwriting attributes (SCD Type 1).
7. Existing surrogate keys never change; new keys take the next value.
8. Record rows_in, rows_out, execution status, and errors.
9. Commit the transaction and release the advisory lock.

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
Incremental SCD Type 1 upsert driven by the loaded_at watermark.
Rerunning a batch yields the same state.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS gold.dim_trailer (
    trailer_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trailer_id VARCHAR NOT NULL,
    trailer_number BIGINT NOT NULL,
    trailer_type VARCHAR NOT NULL,
    length_feet BIGINT NOT NULL,
    model_year BIGINT NOT NULL,
    vin VARCHAR NOT NULL,
    acquisition_date DATE NOT NULL,
    status VARCHAR NOT NULL,
    current_location VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    gold_batch_id TEXT,
    gold_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    gold_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_trailer_business_key UNIQUE (trailer_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_trailer_loaded_at
ON gold.dim_trailer (loaded_at DESC);

CREATE OR REPLACE PROCEDURE gold.load_dim_trailers(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'gold.load_dim_trailers';
    c_target CONSTANT VARCHAR := 'gold.dim_trailer';
    c_lock_key CONSTANT BIGINT := hashtextextended('gold.load_dim_trailers', 0);
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

        INSERT INTO gold.dim_trailer (
            trailer_sk,
            trailer_id,
            trailer_number,
            trailer_type,
            length_feet,
            model_year,
            vin,
            acquisition_date,
            status,
            current_location,
            loaded_at,
            gold_batch_id
        )
        OVERRIDING SYSTEM VALUE
        VALUES (
            -1,
            'UNKNOWN',
            0,
            'UNKNOWN',
            0,
            0,
            'UNKNOWN',
            '1900-01-01',
            'UNKNOWN',
            'Unknown',
            '1900-01-01 00:00:00+00',
            p_batch_id
        )
        ON CONFLICT (trailer_sk) DO NOTHING;

        CREATE TEMP TABLE stg_dim_trailer ON COMMIT DROP AS
        SELECT
            s.trailer_id,
            s.trailer_number,
            s.trailer_type,
            s.length_feet,
            s.model_year,
            s.vin,
            s.acquisition_date,
            s.status,
            s.current_location,
            s.loaded_at
        FROM silver.trailers AS s
        WHERE s.loaded_at >= COALESCE(
            (
                SELECT MAX(d.loaded_at)
                FROM gold.dim_trailer AS d
                WHERE d.trailer_sk <> -1
            ),
            '-infinity'::TIMESTAMPTZ
        );

        SELECT COUNT(*)
        INTO v_rows_in
        FROM stg_dim_trailer;

        INSERT INTO gold.dim_trailer AS t (
            trailer_id,
            trailer_number,
            trailer_type,
            length_feet,
            model_year,
            vin,
            acquisition_date,
            status,
            current_location,
            loaded_at,
            gold_batch_id
        )
        SELECT
            trailer_id,
            trailer_number,
            trailer_type,
            length_feet,
            model_year,
            vin,
            acquisition_date,
            status,
            current_location,
            loaded_at,
            p_batch_id
        FROM stg_dim_trailer
        ON CONFLICT (trailer_id) DO UPDATE
        SET
            trailer_number = EXCLUDED.trailer_number,
            trailer_type = EXCLUDED.trailer_type,
            length_feet = EXCLUDED.length_feet,
            model_year = EXCLUDED.model_year,
            vin = EXCLUDED.vin,
            acquisition_date = EXCLUDED.acquisition_date,
            status = EXCLUDED.status,
            current_location = EXCLUDED.current_location,
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

GRANT ALL ON gold.dim_trailer TO etl_writer;
GRANT SELECT ON gold.dim_trailer TO dq_runner;
GRANT SELECT ON gold.dim_trailer TO analyst_ro;
GRANT SELECT ON gold.dim_trailer TO dashboard_ro;
GRANT USAGE, SELECT ON SEQUENCE gold.dim_trailer_trailer_sk_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE gold.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE gold.load_dim_trailers(TEXT) TO etl_writer;

COMMIT;
