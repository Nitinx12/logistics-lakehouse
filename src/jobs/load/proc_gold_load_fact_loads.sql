/*
================================================================================
Procedure : gold.load_fact_loads
Purpose   : Incrementally upsert one row per load with dimension keys.

Source    : silver.loads
Target    : gold.fact_loads
Grain     : One row per load_id

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated silver rows using the loaded_at watermark.
5. Resolve customer, route, and date keys, falling back to unknown (-1).
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

CREATE TABLE IF NOT EXISTS gold.fact_loads (
    load_id VARCHAR PRIMARY KEY,
    customer_sk BIGINT NOT NULL REFERENCES gold.dim_customer (customer_sk),
    route_sk BIGINT NOT NULL REFERENCES gold.dim_route (route_sk),
    load_date_key INTEGER NOT NULL REFERENCES gold.dim_date (date_key),
    load_type VARCHAR NOT NULL,
    weight_lbs BIGINT NOT NULL,
    pieces BIGINT NOT NULL,
    revenue DOUBLE PRECISION NOT NULL,
    fuel_surcharge DOUBLE PRECISION NOT NULL,
    accessorial_charges BIGINT NOT NULL,
    load_status VARCHAR NOT NULL,
    booking_type VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    gold_batch_id TEXT,
    gold_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    gold_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_loads_customer_sk
ON gold.fact_loads (customer_sk);

CREATE INDEX IF NOT EXISTS idx_fact_loads_route_sk
ON gold.fact_loads (route_sk);

CREATE INDEX IF NOT EXISTS idx_fact_loads_load_date_key
ON gold.fact_loads (load_date_key);

CREATE OR REPLACE PROCEDURE gold.load_fact_loads(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'gold.load_fact_loads';
    c_target CONSTANT VARCHAR := 'gold.fact_loads';
    c_lock_key CONSTANT BIGINT := hashtextextended('gold.load_fact_loads', 0);
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

        CREATE TEMP TABLE stg_fact_loads ON COMMIT DROP AS
        SELECT
            s.load_id,
            COALESCE(dc.customer_sk, -1) AS customer_sk,
            COALESCE(dr.route_sk, -1) AS route_sk,
            COALESCE(dd.date_key, -1) AS load_date_key,
            s.load_type,
            s.weight_lbs,
            s.pieces,
            s.revenue,
            s.fuel_surcharge,
            s.accessorial_charges,
            s.load_status,
            s.booking_type,
            s.loaded_at
        FROM silver.loads AS s
        LEFT JOIN gold.dim_customer AS dc
            ON dc.customer_id = s.customer_id
        LEFT JOIN gold.dim_route AS dr
            ON dr.route_id = s.route_id
        LEFT JOIN gold.dim_date AS dd
            ON dd.full_date = s.load_date
        WHERE s.loaded_at >= COALESCE(
            (SELECT MAX(f.loaded_at) FROM gold.fact_loads AS f),
            '-infinity'::TIMESTAMPTZ
        );

        SELECT COUNT(*)
        INTO v_rows_in
        FROM stg_fact_loads;

        INSERT INTO gold.fact_loads AS t (
            load_id,
            customer_sk,
            route_sk,
            load_date_key,
            load_type,
            weight_lbs,
            pieces,
            revenue,
            fuel_surcharge,
            accessorial_charges,
            load_status,
            booking_type,
            loaded_at,
            gold_batch_id
        )
        SELECT
            load_id,
            customer_sk,
            route_sk,
            load_date_key,
            load_type,
            weight_lbs,
            pieces,
            revenue,
            fuel_surcharge,
            accessorial_charges,
            load_status,
            booking_type,
            loaded_at,
            p_batch_id
        FROM stg_fact_loads
        ON CONFLICT (load_id) DO UPDATE
        SET
            customer_sk = EXCLUDED.customer_sk,
            route_sk = EXCLUDED.route_sk,
            load_date_key = EXCLUDED.load_date_key,
            load_type = EXCLUDED.load_type,
            weight_lbs = EXCLUDED.weight_lbs,
            pieces = EXCLUDED.pieces,
            revenue = EXCLUDED.revenue,
            fuel_surcharge = EXCLUDED.fuel_surcharge,
            accessorial_charges = EXCLUDED.accessorial_charges,
            load_status = EXCLUDED.load_status,
            booking_type = EXCLUDED.booking_type,
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

GRANT ALL ON gold.fact_loads TO etl_writer;
GRANT SELECT ON gold.fact_loads TO dq_runner;
GRANT SELECT ON gold.fact_loads TO analyst_ro;
GRANT SELECT ON gold.fact_loads TO dashboard_ro;
GRANT USAGE, SELECT ON SEQUENCE gold.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE gold.load_fact_loads(TEXT) TO etl_writer;

COMMIT;
