/*
================================================================================
Procedure : silver.load_trucks
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            incrementally upsert truck data from bronze into silver.

Source    : bronze.trucks
Target    : silver.trucks
Quarantine: dq.quarantine_trucks

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated bronze records using the loaded_at watermark.
5. Standardize text fields and safely cast dates and integers.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Deduplicate records by truck_id, keeping the latest loaded_at record.
9. Incrementally upsert valid records into silver.trucks.
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
Invalid records are stored in dq.quarantine_trucks.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS silver.trucks (
    truck_id VARCHAR PRIMARY KEY,
    unit_number BIGINT NOT NULL,
    make VARCHAR NOT NULL,
    model_year BIGINT NOT NULL,
    vin VARCHAR NOT NULL,
    acquisition_date DATE NOT NULL,
    acquisition_mileage BIGINT NOT NULL,
    fuel_type VARCHAR NOT NULL,
    tank_capacity_gallons BIGINT NOT NULL,
    status VARCHAR NOT NULL,
    home_terminal VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_trucks_amounts
    CHECK (
        unit_number > 0
        AND model_year > 0
        AND acquisition_mileage >= 0
        AND tank_capacity_gallons > 0
    )
);

CREATE INDEX IF NOT EXISTS idx_silver_trucks_loaded_at
ON silver.trucks (loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_trucks_status
ON silver.trucks (status);

CREATE TABLE IF NOT EXISTS dq.quarantine_trucks (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_trucks_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_trucks(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_trucks';
    c_target CONSTANT VARCHAR := 'silver.trucks';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_trucks', 0);
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

        CREATE TEMP TABLE stg_trucks ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY truck_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.trucks
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.truck_id) AS truck_id,
                silver.try_int(b.unit_number)::BIGINT AS unit_number,
                INITCAP(silver.clean_text(b.make)) AS make,
                silver.try_int(b.model_year)::BIGINT AS model_year,
                silver.clean_text(b.vin) AS vin,
                silver.try_timestamp(b.acquisition_date)::DATE
                    AS acquisition_date,
                silver.try_int(b.acquisition_mileage)::BIGINT
                    AS acquisition_mileage,
                UPPER(silver.clean_text(b.fuel_type)) AS fuel_type,
                silver.try_int(b.tank_capacity_gallons)::BIGINT
                    AS tank_capacity_gallons,
                UPPER(silver.clean_text(b.status)) AS status,
                INITCAP(silver.clean_text(b.home_terminal)) AS home_terminal,
                l.loaded_ts AS loaded_at
            FROM deduplicated AS b
            CROSS JOIN LATERAL (
                SELECT silver.try_timestamptz(b.loaded_at) AS loaded_ts
            ) AS l
            WHERE b.rnk = 1
                AND (
                    l.loaded_ts IS NULL
                    OR l.loaded_ts >= COALESCE(
                        (SELECT MAX(s.loaded_at) FROM silver.trucks AS s),
                        '-infinity'::TIMESTAMPTZ
                    )
                )
        )
        SELECT
            cleaned.*,
            NULLIF(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN truck_id IS NULL THEN 'truck_id' END,
                        CASE WHEN unit_number IS NULL OR unit_number <= 0
                            THEN 'unit_number' END,
                        CASE WHEN make IS NULL THEN 'make' END,
                        CASE WHEN model_year IS NULL OR model_year <= 0
                            THEN 'model_year' END,
                        CASE WHEN vin IS NULL THEN 'vin' END,
                        CASE WHEN acquisition_date IS NULL
                            THEN 'acquisition_date' END,
                        CASE WHEN acquisition_mileage IS NULL
                            OR acquisition_mileage < 0
                            THEN 'acquisition_mileage' END,
                        CASE WHEN fuel_type IS NULL THEN 'fuel_type' END,
                        CASE WHEN tank_capacity_gallons IS NULL
                            OR tank_capacity_gallons <= 0
                            THEN 'tank_capacity_gallons' END,
                        CASE WHEN status IS NULL THEN 'status' END,
                        CASE WHEN home_terminal IS NULL THEN 'home_terminal' END,
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
        FROM stg_trucks;

        INSERT INTO dq.quarantine_trucks (
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
        FROM stg_trucks
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        INSERT INTO silver.trucks AS t (
            truck_id,
            unit_number,
            make,
            model_year,
            vin,
            acquisition_date,
            acquisition_mileage,
            fuel_type,
            tank_capacity_gallons,
            status,
            home_terminal,
            loaded_at,
            silver_batch_id
        )
        SELECT DISTINCT ON (truck_id)
            truck_id,
            unit_number,
            make,
            model_year,
            vin,
            acquisition_date,
            acquisition_mileage,
            fuel_type,
            tank_capacity_gallons,
            status,
            home_terminal,
            loaded_at,
            p_batch_id
        FROM stg_trucks
        WHERE reject_reason IS NULL
        ORDER BY
            truck_id,
            loaded_at DESC
        ON CONFLICT (truck_id) DO UPDATE
        SET
            unit_number = EXCLUDED.unit_number,
            make = EXCLUDED.make,
            model_year = EXCLUDED.model_year,
            vin = EXCLUDED.vin,
            acquisition_date = EXCLUDED.acquisition_date,
            acquisition_mileage = EXCLUDED.acquisition_mileage,
            fuel_type = EXCLUDED.fuel_type,
            tank_capacity_gallons = EXCLUDED.tank_capacity_gallons,
            status = EXCLUDED.status,
            home_terminal = EXCLUDED.home_terminal,
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

GRANT ALL ON silver.trucks TO etl_writer;
GRANT SELECT ON silver.trucks TO dq_runner;
GRANT ALL ON dq.quarantine_trucks TO etl_writer;
GRANT SELECT ON dq.quarantine_trucks TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_trucks_id_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_trucks(TEXT) TO etl_writer;

COMMIT;
