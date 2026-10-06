/*
================================================================================
Procedure : silver.load_safety_incidents
Purpose   : Clean, validate, deduplicate, and load safety incident data
            from bronze.safety_incidents into silver.safety_incidents.

Source    : bronze.safety_incidents
Target    : silver.safety_incidents

Process
--------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create a new ETL audit log entry for the current batch.
4. Read all rows from the bronze safety_incidents table.
5. Deduplicate records using incident_id, keeping the latest loaded_at record.
6. Standardize and cast dates, timestamps, Boolean flags, text, and numeric fields.
7. Validate mandatory fields and ensure monetary amounts are non-negative.
8. Reject invalid records and record the rejection count.
9. Refuse to truncate the silver table when all source rows are invalid.
10. Truncate the existing silver table for a full-refresh load.
11. Insert only validated records into silver.safety_incidents.
12. Record rows_in, rows_out, rows_rejected, execution status, and errors.
13. Commit the transaction and release the advisory lock.

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
Full refresh: silver.safety_incidents is truncated before loading
validated records from bronze.
================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS silver.safety_incidents(
    incident_id             VARCHAR              PRIMARY KEY,
    trip_id                 VARCHAR              NOT NULL,
    truck_id                VARCHAR,
    driver_id               VARCHAR,
    incident_date           DATE                 NOT NULL,
    incident_type           VARCHAR              NOT NULL,
    location_city           VARCHAR              NOT NULL,
    location_state          VARCHAR              NOT NULL,
    at_fault_flag           BOOLEAN              NOT NULL,
    injury_flag             BOOLEAN              NOT NULL,
    vehicle_damage_cost     NUMERIC(14, 2)       NOT NULL,
    cargo_damage_cost       NUMERIC(14, 2)       NOT NULL,
    claim_amount            NUMERIC(14, 2)       NOT NULL,
    preventable_flag        BOOLEAN              NOT NULL,
    description             TEXT                 NOT NULL,
    loaded_at               TIMESTAMPTZ          NOT NULL,
    silver_batch_id         TEXT,
    silver_loaded_at        TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_safety_incidents_amounts
        CHECK (
            vehicle_damage_cost >= 0
            AND cargo_damage_cost >= 0
            AND claim_amount >= 0
        )
);

CREATE INDEX IF NOT EXISTS idx_silver_safety_incidents_trip_id
    ON silver.safety_incidents (trip_id);

CREATE INDEX IF NOT EXISTS idx_silver_safety_incidents_incident_date
    ON silver.safety_incidents (incident_date);

CREATE OR REPLACE PROCEDURE silver.load_safety_incidents(
    p_batch_id TEXT
)

LANGUAGE plpgsql
AS $$

DECLARE
    c_procedure CONSTANT    VARCHAR := 'silver.load_safety_incidents';
    c_target CONSTANT       VARCHAR := 'silver.safety_incidents';
    c_lock_key CONSTANT     BIGINT := hashtextextended('silver.load_safety_incidents', 0);
    v_log_id                BIGINT;
    v_rows_in               BIGINT := 0;
    v_rows_out              BIGINT := 0;
    v_rows_rejected         BIGINT := 0;
    v_error                 TEXT;
    v_sqlstate              TEXT;

BEGIN
    IF NOT pg_try_advisory_lock(c_lock_key) THEN
        RAISE EXCEPTION '% is already running', c_procedure
            USING ERRCODE = '55P03';
    END IF;

    UPDATE silver.etl_logs
    SET
        status = 'FAILED',
        error_message = 'Orphaned run: session ended before completion',
        finished_at = NOW()
    WHERE procedure_name = c_procedure
        AND target_table = c_target
        AND status = 'STARTED';

    INSERT INTO silver.etl_logs (
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

        SELECT COUNT(*) INTO v_rows_in
        FROM bronze.safety_incidents;

        CREATE TEMP TABLE stg_safety_incidents ON COMMIT DROP AS
        WITH deduplicated AS(
            SELECT
                *,
                ROW_NUMBER() OVER(
                    PARTITION BY incident_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.safety_incidents
        ),
        cleaned AS(
            SELECT
                incident_id,
                trip_id,
                truck_id,
                driver_id,
                CASE
                    WHEN incident_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN incident_date::DATE
                    WHEN incident_date ~ '^[0-9]{8}$' AND incident_date <> '00000000'
                        THEN TO_DATE(incident_date, 'YYYYMMDD')
                    ELSE NULL
                END AS incident_date,
                TRIM(incident_type) AS incident_type,
                TRIM(location_city) AS location_city,
                UPPER(TRIM(location_state)) AS location_state,
                CASE
                    WHEN LOWER(at_fault_flag) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                    WHEN LOWER(at_fault_flag) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
                END AS at_fault_flag,
                CASE
                    WHEN LOWER(injury_flag) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                    WHEN LOWER(injury_flag) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
                END AS injury_flag,
                CASE
                    WHEN vehicle_damage_cost ~ '^-?\d+(\.\d+)?$' THEN vehicle_damage_cost::NUMERIC(14, 2)
                END AS vehicle_damage_cost,
                CASE
                    WHEN cargo_damage_cost ~ '^-?\d+(\.\d+)?$' THEN cargo_damage_cost::NUMERIC(14, 2)
                END AS cargo_damage_cost,
                CASE
                    WHEN claim_amount ~ '^-?\d+(\.\d+)?$' THEN claim_amount::NUMERIC(14, 2)
                END AS claim_amount,
                CASE
                    WHEN LOWER(preventable_flag) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                    WHEN LOWER(preventable_flag) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
                END AS preventable_flag,
                TRIM(description) AS description,
                CASE
                    WHEN loaded_at ~ '^\d{4}-\d{2}-\d{2}' THEN loaded_at::TIMESTAMPTZ
                END AS loaded_at
            FROM deduplicated
            WHERE rnk = 1
        )
        SELECT
            cleaned.*,
            (
                incident_id IS NOT NULL
                AND trip_id IS NOT NULL
                AND incident_date IS NOT NULL
                AND incident_type IS NOT NULL
                AND location_city IS NOT NULL
                AND location_state IS NOT NULL
                AND at_fault_flag IS NOT NULL
                AND injury_flag IS NOT NULL
                AND vehicle_damage_cost >= 0
                AND cargo_damage_cost >= 0
                AND claim_amount >= 0
                AND preventable_flag IS NOT NULL
                AND description IS NOT NULL
                AND loaded_at IS NOT NULL
            ) AS is_valid
        FROM cleaned;

        SELECT COUNT(*) FILTER (WHERE is_valid IS NOT TRUE) INTO v_rows_rejected
        FROM stg_safety_incidents;

        IF v_rows_in = v_rows_rejected THEN
            RAISE EXCEPTION 'No valid rows in bronze.safety_incidents
                (rows_in=%),
                refusing to truncate %',
                v_rows_in,
                c_target;
        END IF;

        TRUNCATE TABLE silver.safety_incidents;

        INSERT INTO silver.safety_incidents (
            incident_id,
            trip_id,
            truck_id,
            driver_id,
            incident_date,
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
            silver_batch_id
        )
        SELECT DISTINCT ON (incident_id)
            incident_id,
            trip_id,
            truck_id,
            driver_id,
            incident_date,
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
        FROM stg_safety_incidents
        WHERE is_valid
        ORDER BY
            incident_id,
            loaded_at DESC;

        GET DIAGNOSTICS v_rows_out = ROW_COUNT;

        UPDATE silver.etl_logs
        SET
            status = 'SUCCESS',
            rows_in = v_rows_in,
            rows_out = v_rows_out,
            rows_rejected = v_rows_rejected,
            finished_at = NOW()
        WHERE id = v_log_id;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error = MESSAGE_TEXT,
                v_sqlstate = RETURNED_SQLSTATE;
    END;

    IF v_error IS NOT NULL THEN
        UPDATE silver.etl_logs
        SET
            status = 'FAILED',
            rows_in = v_rows_in,
            rows_rejected = v_rows_rejected,
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

GRANT ALL ON silver.safety_incidents TO etl_writer;
GRANT SELECT ON silver.safety_incidents TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_safety_incidents(TEXT) TO etl_writer;

COMMIT;

