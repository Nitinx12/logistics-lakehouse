/*
================================================================================
Procedure : silver.load_maintenance_records
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            incrementally upsert maintenance record data from bronze into silver.

Source    : bronze.maintenance_records
Target    : silver.maintenance_records
Quarantine: dq.quarantine_maintenance_records

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated bronze records using the loaded_at watermark.
5. Standardize text fields and safely cast dates and numerics.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Deduplicate records by maintenance_id, keeping the latest loaded_at record.
9. Incrementally upsert valid records into silver.maintenance_records.
10. Update an existing record only when the incoming loaded_at is newer.
11. Record rows_in, rows_out, rows_rejected, execution status, and errors.
12. Commit the transaction and release the advisory lock.

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
Incremental upsert (SCD Type 1) driven by the loaded_at watermark.
Existing records are updated only when the incoming loaded_at is newer.
Invalid records are stored in dq.quarantine_maintenance_records.

================================================================================
*/

BEGIN;

CREATE OR REPLACE FUNCTION silver.try_numeric(p_value TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
BEGIN
    IF p_value IS NULL OR p_value !~ '^[+-]?\d+(\.\d+)?$' THEN
        RETURN NULL;
    END IF;
    RETURN p_value::NUMERIC;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;

CREATE TABLE IF NOT EXISTS silver.maintenance_records (
    maintenance_id VARCHAR PRIMARY KEY,
    truck_id VARCHAR NOT NULL,
    maintenance_date DATE NOT NULL,
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
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_maintenance_records_amounts
        CHECK (
            odometer_reading >= 0
            AND labor_hours >= 0
            AND labor_cost >= 0
            AND parts_cost >= 0
            AND total_cost >= 0
            AND downtime_hours >= 0
        )
);

CREATE INDEX IF NOT EXISTS idx_silver_maintenance_records_loaded_at
    ON silver.maintenance_records (loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_maintenance_records_truck_id
    ON silver.maintenance_records (truck_id);

CREATE INDEX IF NOT EXISTS idx_silver_maintenance_records_maintenance_date
    ON silver.maintenance_records (maintenance_date);

CREATE TABLE IF NOT EXISTS dq.quarantine_maintenance_records (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_maintenance_records_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_maintenance_records(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_maintenance_records';
    c_target CONSTANT VARCHAR := 'silver.maintenance_records';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_maintenance_records', 0);
    v_log_id BIGINT;
    v_rows_in BIGINT := 0;
    v_rows_out BIGINT := 0;
    v_rows_rejected BIGINT := 0;
    v_error TEXT;
    v_sqlstate TEXT;
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

        CREATE TEMP TABLE stg_maintenance_records ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY maintenance_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.maintenance_records
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.maintenance_id) AS maintenance_id,
                silver.clean_text(b.truck_id) AS truck_id,
                silver.try_timestamp(b.maintenance_date)::DATE AS maintenance_date,
                UPPER(silver.clean_text(b.maintenance_type)) AS maintenance_type,
                silver.clean_text(b.service_description) AS service_description,
                INITCAP(silver.clean_text(b.facility_location)) AS facility_location,
                silver.try_int(b.odometer_reading)::BIGINT AS odometer_reading,
                silver.try_numeric(b.labor_hours) AS labor_hours,
                silver.try_numeric(b.labor_cost) AS labor_cost,
                silver.try_numeric(b.parts_cost) AS parts_cost,
                silver.try_numeric(b.total_cost) AS total_cost,
                silver.try_numeric(b.downtime_hours) AS downtime_hours,
                l.loaded_ts AS loaded_at
            FROM deduplicated AS b
            CROSS JOIN LATERAL (
                SELECT silver.try_timestamptz(b.loaded_at) AS loaded_ts
            ) AS l
            WHERE b.rnk = 1
                AND (
                    l.loaded_ts IS NULL
                    OR l.loaded_ts >= COALESCE(
                        (SELECT MAX(s.loaded_at) FROM silver.maintenance_records AS s),
                        '-infinity'::TIMESTAMPTZ
                    )
                )
        )
        SELECT
            cleaned.*,
            NULLIF(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN maintenance_id IS NULL THEN 'maintenance_id' END,
                        CASE WHEN truck_id IS NULL THEN 'truck_id' END,
                        CASE WHEN maintenance_date IS NULL THEN 'maintenance_date' END,
                        CASE WHEN maintenance_type IS NULL THEN 'maintenance_type' END,
                        CASE WHEN service_description IS NULL THEN 'service_description' END,
                        CASE WHEN facility_location IS NULL THEN 'facility_location' END,
                        CASE WHEN odometer_reading IS NULL OR odometer_reading < 0 THEN 'odometer_reading' END,
                        CASE WHEN labor_hours IS NULL OR labor_hours < 0 THEN 'labor_hours' END,
                        CASE WHEN labor_cost IS NULL OR labor_cost < 0 THEN 'labor_cost' END,
                        CASE WHEN parts_cost IS NULL OR parts_cost < 0 THEN 'parts_cost' END,
                        CASE WHEN total_cost IS NULL OR total_cost < 0 THEN 'total_cost' END,
                        CASE WHEN downtime_hours IS NULL OR downtime_hours < 0 THEN 'downtime_hours' END,
                        CASE WHEN loaded_at IS NULL THEN 'loaded_at' END
                    ],
                    ','
                ),
                ''
            ) AS reject_reason
        FROM cleaned;

        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE reject_reason IS NOT NULL)
        INTO v_rows_in, v_rows_rejected
        FROM stg_maintenance_records;

        INSERT INTO dq.quarantine_maintenance_records (
            batch_id,
            row_hash,
            reject_reason,
            source_row
        )
        SELECT
            p_batch_id,
            MD5(source_row::TEXT),
            reject_reason,
            source_row
        FROM stg_maintenance_records
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        INSERT INTO silver.maintenance_records AS t (
            maintenance_id,
            truck_id,
            maintenance_date,
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
            silver_batch_id
        )
        SELECT DISTINCT ON (maintenance_id)
            maintenance_id,
            truck_id,
            maintenance_date,
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
        FROM stg_maintenance_records
        WHERE reject_reason IS NULL
        ORDER BY
            maintenance_id,
            loaded_at DESC
        ON CONFLICT (maintenance_id) DO UPDATE
        SET
            truck_id = EXCLUDED.truck_id,
            maintenance_date = EXCLUDED.maintenance_date,
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
            silver_batch_id = EXCLUDED.silver_batch_id,
            silver_updated_at = NOW()
        WHERE t.loaded_at < EXCLUDED.loaded_at;

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

GRANT ALL ON silver.maintenance_records TO etl_writer;
GRANT SELECT ON silver.maintenance_records TO dq_runner;
GRANT ALL ON dq.quarantine_maintenance_records TO etl_writer;
GRANT SELECT ON dq.quarantine_maintenance_records TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_maintenance_records_id_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_maintenance_records(TEXT) TO etl_writer;

COMMIT;
