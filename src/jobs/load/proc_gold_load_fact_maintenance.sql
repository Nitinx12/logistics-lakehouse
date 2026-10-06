/*
================================================================================
Procedure : gold.load_fact_maintenance
Purpose   : Incrementally upsert one row per maintenance record with keys.

Source    : silver.maintenance_records
Target    : gold.fact_maintenance
Grain     : One row per maintenance_id

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated silver rows using the loaded_at watermark.
5. Resolve truck and date keys, unknown (-1) on misses.
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

CREATE TABLE IF NOT EXISTS gold.fact_maintenance (
    maintenance_id VARCHAR PRIMARY KEY,
    truck_sk BIGINT NOT NULL REFERENCES gold.dim_truck (truck_sk),
    maintenance_date_key INTEGER NOT NULL REFERENCES gold.dim_date (date_key),
    maintenance_type VARCHAR NOT NULL,
    service_description TEXT NOT NULL,
    facility_location VARCHAR NOT NULL,
    odometer_reading BIGINT NOT NULL,
    labor_hours NUMERIC(10, 2) NOT NULL,
    labor_cost NUMERIC(14, 2) NOT NULL,
    parts_cost NUMERIC(14, 2) NOT NULL,
    total_cost NUMERIC(14, 2) NOT NULL,
    downtime_hours NUMERIC(10, 2) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    gold_batch_id TEXT,
    gold_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    gold_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_maintenance_truck_sk
ON gold.fact_maintenance (truck_sk);

CREATE OR REPLACE PROCEDURE gold.load_fact_maintenance(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'gold.load_fact_maintenance';
    c_target CONSTANT VARCHAR := 'gold.fact_maintenance';
    c_lock_key CONSTANT BIGINT := hashtextextended('gold.load_fact_maintenance', 0);
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

        CREATE TEMP TABLE stg_fact_maint ON COMMIT DROP AS
        SELECT
            s.maintenance_id,
            COALESCE(dt.truck_sk, -1) AS truck_sk,
            COALESCE(d.date_key, -1) AS maintenance_date_key,
            s.maintenance_type,
            s.service_description,
            s.facility_location,
            s.odometer_reading,
            s.labor_hours,
            s.labor_cost,
            s.parts_cost,
            s.total_cost,
            s.downtime_hours,
            s.loaded_at
        FROM silver.maintenance_records AS s
        LEFT JOIN gold.dim_truck AS dt
            ON dt.truck_id = s.truck_id
        LEFT JOIN gold.dim_date AS d
            ON d.full_date = s.maintenance_date
        WHERE s.loaded_at >= COALESCE(
            (SELECT MAX(f.loaded_at) FROM gold.fact_maintenance AS f),
            '-infinity'::TIMESTAMPTZ
        );

        SELECT COUNT(*)
        INTO v_rows_in
        FROM stg_fact_maint;

        INSERT INTO gold.fact_maintenance AS t (
            maintenance_id,
            truck_sk,
            maintenance_date_key,
            maintenance_type,
            service_description,
            facility_location,
            odometer_reading,
            labor_hours,
            labor_cost,
            parts_cost,
            total_cost,
            downtime_hours,
            loaded_at,
            gold_batch_id
        )
        SELECT
            maintenance_id,
            truck_sk,
            maintenance_date_key,
            maintenance_type,
            service_description,
            facility_location,
            odometer_reading,
            labor_hours,
            labor_cost,
            parts_cost,
            total_cost,
            downtime_hours,
            loaded_at,
            p_batch_id
        FROM stg_fact_maint
        ON CONFLICT (maintenance_id) DO UPDATE
        SET
            truck_sk = EXCLUDED.truck_sk,
            maintenance_date_key = EXCLUDED.maintenance_date_key,
            maintenance_type = EXCLUDED.maintenance_type,
            service_description = EXCLUDED.service_description,
            facility_location = EXCLUDED.facility_location,
            odometer_reading = EXCLUDED.odometer_reading,
            labor_hours = EXCLUDED.labor_hours,
            labor_cost = EXCLUDED.labor_cost,
            parts_cost = EXCLUDED.parts_cost,
            total_cost = EXCLUDED.total_cost,
            downtime_hours = EXCLUDED.downtime_hours,
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

GRANT ALL ON gold.fact_maintenance TO etl_writer;
GRANT SELECT ON gold.fact_maintenance TO dq_runner;
GRANT SELECT ON gold.fact_maintenance TO analyst_ro;
GRANT SELECT ON gold.fact_maintenance TO dashboard_ro;
GRANT USAGE, SELECT ON SEQUENCE gold.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE gold.load_fact_maintenance(TEXT) TO etl_writer;

COMMIT;
