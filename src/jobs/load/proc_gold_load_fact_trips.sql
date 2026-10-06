/*
================================================================================
Procedure : gold.load_fact_trips
Purpose   : Incrementally upsert one row per trip with dimension keys.

Source    : silver.trips
Target    : gold.fact_trips
Grain     : One row per trip_id

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated silver rows using the loaded_at watermark.
5. Resolve driver, truck, trailer, and date keys, unknown (-1) on misses.
6. Keep the source load_id as a degenerate dimension.
7. Upsert rows on the grain key, keeping the newer loaded_at row.
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
Incremental upsert on the grain key driven by the loaded_at watermark.
Rerunning a batch yields the same state.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS gold.fact_trips (
    trip_id VARCHAR PRIMARY KEY,
    load_id VARCHAR NOT NULL,
    driver_sk BIGINT NOT NULL REFERENCES gold.dim_driver (driver_sk),
    truck_sk BIGINT NOT NULL REFERENCES gold.dim_truck (truck_sk),
    trailer_sk BIGINT NOT NULL REFERENCES gold.dim_trailer (trailer_sk),
    dispatch_date_key INTEGER NOT NULL REFERENCES gold.dim_date (date_key),
    actual_distance_miles BIGINT NOT NULL,
    actual_duration_hours DOUBLE PRECISION NOT NULL,
    fuel_gallons_used DOUBLE PRECISION NOT NULL,
    average_mpg DOUBLE PRECISION NOT NULL,
    idle_time_hours DOUBLE PRECISION NOT NULL,
    trip_status VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    gold_batch_id TEXT,
    gold_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    gold_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_trips_load_id
ON gold.fact_trips (load_id);

CREATE INDEX IF NOT EXISTS idx_fact_trips_driver_sk
ON gold.fact_trips (driver_sk);

CREATE INDEX IF NOT EXISTS idx_fact_trips_dispatch_date_key
ON gold.fact_trips (dispatch_date_key);

CREATE OR REPLACE PROCEDURE gold.load_fact_trips(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'gold.load_fact_trips';
    c_target CONSTANT VARCHAR := 'gold.fact_trips';
    c_lock_key CONSTANT BIGINT := hashtextextended('gold.load_fact_trips', 0);
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

        CREATE TEMP TABLE stg_fact_trips ON COMMIT DROP AS
        SELECT
            s.trip_id,
            s.load_id,
            COALESCE(dd.driver_sk, -1) AS driver_sk,
            COALESCE(dt.truck_sk, -1) AS truck_sk,
            COALESCE(dl.trailer_sk, -1) AS trailer_sk,
            COALESCE(d.date_key, -1) AS dispatch_date_key,
            s.actual_distance_miles,
            s.actual_duration_hours,
            s.fuel_gallons_used,
            s.average_mpg,
            s.idle_time_hours,
            s.trip_status,
            s.loaded_at
        FROM silver.trips AS s
        LEFT JOIN gold.dim_driver AS dd
            ON dd.driver_id = s.driver_id
        LEFT JOIN gold.dim_truck AS dt
            ON dt.truck_id = s.truck_id
        LEFT JOIN gold.dim_trailer AS dl
            ON dl.trailer_id = s.trailer_id
        LEFT JOIN gold.dim_date AS d
            ON d.full_date = s.dispatch_date
        WHERE s.loaded_at >= COALESCE(
            (SELECT MAX(f.loaded_at) FROM gold.fact_trips AS f),
            '-infinity'::TIMESTAMPTZ
        );

        SELECT COUNT(*)
        INTO v_rows_in
        FROM stg_fact_trips;

        INSERT INTO gold.fact_trips AS t (
            trip_id,
            load_id,
            driver_sk,
            truck_sk,
            trailer_sk,
            dispatch_date_key,
            actual_distance_miles,
            actual_duration_hours,
            fuel_gallons_used,
            average_mpg,
            idle_time_hours,
            trip_status,
            loaded_at,
            gold_batch_id
        )
        SELECT
            trip_id,
            load_id,
            driver_sk,
            truck_sk,
            trailer_sk,
            dispatch_date_key,
            actual_distance_miles,
            actual_duration_hours,
            fuel_gallons_used,
            average_mpg,
            idle_time_hours,
            trip_status,
            loaded_at,
            p_batch_id
        FROM stg_fact_trips
        ON CONFLICT (trip_id) DO UPDATE
        SET
            load_id = EXCLUDED.load_id,
            driver_sk = EXCLUDED.driver_sk,
            truck_sk = EXCLUDED.truck_sk,
            trailer_sk = EXCLUDED.trailer_sk,
            dispatch_date_key = EXCLUDED.dispatch_date_key,
            actual_distance_miles = EXCLUDED.actual_distance_miles,
            actual_duration_hours = EXCLUDED.actual_duration_hours,
            fuel_gallons_used = EXCLUDED.fuel_gallons_used,
            average_mpg = EXCLUDED.average_mpg,
            idle_time_hours = EXCLUDED.idle_time_hours,
            trip_status = EXCLUDED.trip_status,
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

GRANT ALL ON gold.fact_trips TO etl_writer;
GRANT SELECT ON gold.fact_trips TO dq_runner;
GRANT SELECT ON gold.fact_trips TO analyst_ro;
GRANT SELECT ON gold.fact_trips TO dashboard_ro;
GRANT USAGE, SELECT ON SEQUENCE gold.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE gold.load_fact_trips(TEXT) TO etl_writer;

COMMIT;
