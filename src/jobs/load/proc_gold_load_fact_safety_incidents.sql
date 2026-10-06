/*
================================================================================
Procedure : gold.load_fact_safety_incidents
Purpose   : Incrementally upsert one row per safety incident with keys.

Source    : silver.safety_incidents
Target    : gold.fact_safety_incidents
Grain     : One row per incident_id

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated silver rows using the loaded_at watermark.
5. Resolve trip, truck, driver, and date keys, unknown (-1) on misses.
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

CREATE TABLE IF NOT EXISTS gold.fact_safety_incidents (
    incident_id VARCHAR PRIMARY KEY,
    trip_id VARCHAR NOT NULL,
    truck_sk BIGINT NOT NULL REFERENCES gold.dim_truck (truck_sk),
    driver_sk BIGINT NOT NULL REFERENCES gold.dim_driver (driver_sk),
    incident_date_key INTEGER NOT NULL REFERENCES gold.dim_date (date_key),
    incident_type VARCHAR NOT NULL,
    location_city VARCHAR NOT NULL,
    location_state VARCHAR NOT NULL,
    at_fault_flag BOOLEAN NOT NULL,
    injury_flag BOOLEAN NOT NULL,
    vehicle_damage_cost NUMERIC(14, 2) NOT NULL,
    cargo_damage_cost NUMERIC(14, 2) NOT NULL,
    claim_amount NUMERIC(14, 2) NOT NULL,
    preventable_flag BOOLEAN NOT NULL,
    description TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    gold_batch_id TEXT,
    gold_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    gold_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_safety_incidents_trip_id
ON gold.fact_safety_incidents (trip_id);

CREATE OR REPLACE PROCEDURE gold.load_fact_safety_incidents(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'gold.load_fact_safety_incidents';
    c_target CONSTANT VARCHAR := 'gold.fact_safety_incidents';
    c_lock_key CONSTANT BIGINT := hashtextextended('gold.load_fact_safety_incidents', 0);
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

        CREATE TEMP TABLE stg_fact_safety ON COMMIT DROP AS
        SELECT
            s.incident_id,
            s.trip_id,
            COALESCE(dt.truck_sk, -1) AS truck_sk,
            COALESCE(dd.driver_sk, -1) AS driver_sk,
            COALESCE(d.date_key, -1) AS incident_date_key,
            s.incident_type,
            s.location_city,
            s.location_state,
            s.at_fault_flag,
            s.injury_flag,
            s.vehicle_damage_cost,
            s.cargo_damage_cost,
            s.claim_amount,
            s.preventable_flag,
            s.description,
            s.loaded_at
        FROM silver.safety_incidents AS s
        LEFT JOIN gold.dim_truck AS dt
            ON dt.truck_id = s.truck_id
        LEFT JOIN gold.dim_driver AS dd
            ON dd.driver_id = s.driver_id
        LEFT JOIN gold.dim_date AS d
            ON d.full_date = s.incident_date
        WHERE s.loaded_at >= COALESCE(
            (SELECT MAX(f.loaded_at) FROM gold.fact_safety_incidents AS f),
            '-infinity'::TIMESTAMPTZ
        );

        SELECT COUNT(*)
        INTO v_rows_in
        FROM stg_fact_safety;

        INSERT INTO gold.fact_safety_incidents AS t (
            incident_id,
            trip_id,
            truck_sk,
            driver_sk,
            incident_date_key,
            incident_type,
            location_city,
            location_state,
            at_fault_flag,
            injury_flag,
            vehicle_damage_cost,
            cargo_damage_cost,
            claim_amount,
            preventable_flag,
            description,
            loaded_at,
            gold_batch_id
        )
        SELECT
            incident_id,
            trip_id,
            truck_sk,
            driver_sk,
            incident_date_key,
            incident_type,
            location_city,
            location_state,
            at_fault_flag,
            injury_flag,
            vehicle_damage_cost,
            cargo_damage_cost,
            claim_amount,
            preventable_flag,
            description,
            loaded_at,
            p_batch_id
        FROM stg_fact_safety
        ON CONFLICT (incident_id) DO UPDATE
        SET
            trip_id = EXCLUDED.trip_id,
            truck_sk = EXCLUDED.truck_sk,
            driver_sk = EXCLUDED.driver_sk,
            incident_date_key = EXCLUDED.incident_date_key,
            incident_type = EXCLUDED.incident_type,
            location_city = EXCLUDED.location_city,
            location_state = EXCLUDED.location_state,
            at_fault_flag = EXCLUDED.at_fault_flag,
            injury_flag = EXCLUDED.injury_flag,
            vehicle_damage_cost = EXCLUDED.vehicle_damage_cost,
            cargo_damage_cost = EXCLUDED.cargo_damage_cost,
            claim_amount = EXCLUDED.claim_amount,
            preventable_flag = EXCLUDED.preventable_flag,
            description = EXCLUDED.description,
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

GRANT ALL ON gold.fact_safety_incidents TO etl_writer;
GRANT SELECT ON gold.fact_safety_incidents TO dq_runner;
GRANT SELECT ON gold.fact_safety_incidents TO analyst_ro;
GRANT SELECT ON gold.fact_safety_incidents TO dashboard_ro;
GRANT USAGE, SELECT ON SEQUENCE gold.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE gold.load_fact_safety_incidents(TEXT) TO etl_writer;

COMMIT;
