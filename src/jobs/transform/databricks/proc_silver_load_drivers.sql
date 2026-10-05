/*
================================================================================
Procedure : silver.load_drivers
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            incrementally upsert driver data from bronze into silver.

Source    : bronze.drivers
Target    : silver.drivers
Quarantine: dq.quarantine_drivers

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated bronze records using the loaded_at watermark.
5. Standardize text fields and safely cast dates and integers.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Deduplicate records by driver_id, keeping the latest loaded_at record.
9. Incrementally upsert valid records into silver.drivers.
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
Invalid records are stored in dq.quarantine_drivers.

Notes
-----
termination_date is null for active drivers (82.67 percent observed).
A termination_date earlier than hire_date is rejected as invalid.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS silver.drivers (
    driver_id VARCHAR PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    hire_date DATE NOT NULL,
    termination_date DATE,
    license_number VARCHAR NOT NULL,
    license_state VARCHAR NOT NULL,
    date_of_birth DATE NOT NULL,
    home_terminal VARCHAR NOT NULL,
    employment_status VARCHAR NOT NULL,
    cdl_class VARCHAR NOT NULL,
    years_experience BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_drivers_experience
    CHECK (years_experience >= 0)
);

CREATE INDEX IF NOT EXISTS idx_silver_drivers_loaded_at
ON silver.drivers (loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_drivers_employment_status
ON silver.drivers (employment_status);

CREATE TABLE IF NOT EXISTS dq.quarantine_drivers (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_drivers_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_drivers(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_drivers';
    c_target CONSTANT VARCHAR := 'silver.drivers';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_drivers', 0);
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

        CREATE TEMP TABLE stg_drivers ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY driver_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.drivers
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.driver_id) AS driver_id,
                silver.clean_text(b.first_name) AS first_name,
                silver.clean_text(b.last_name) AS last_name,
                silver.try_timestamp(b.hire_date)::DATE AS hire_date,
                silver.try_timestamp(b.termination_date)::DATE AS termination_date,
                silver.clean_text(b.license_number) AS license_number,
                UPPER(silver.clean_text(b.license_state)) AS license_state,
                silver.try_timestamp(b.date_of_birth)::DATE AS date_of_birth,
                INITCAP(silver.clean_text(b.home_terminal)) AS home_terminal,
                UPPER(silver.clean_text(b.employment_status)) AS employment_status,
                UPPER(silver.clean_text(b.cdl_class)) AS cdl_class,
                silver.try_int(b.years_experience)::BIGINT AS years_experience,
                l.loaded_ts AS loaded_at
            FROM deduplicated AS b
            CROSS JOIN LATERAL (
                SELECT silver.try_timestamptz(b.loaded_at) AS loaded_ts
            ) AS l
            WHERE b.rnk = 1
                AND (
                    l.loaded_ts IS NULL
                    OR l.loaded_ts >= COALESCE(
                        (SELECT MAX(s.loaded_at) FROM silver.drivers AS s),
                        '-infinity'::TIMESTAMPTZ
                    )
                )
        )
        SELECT
            cleaned.*,
            NULLIF(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN driver_id IS NULL THEN 'driver_id' END,
                        CASE WHEN first_name IS NULL THEN 'first_name' END,
                        CASE WHEN last_name IS NULL THEN 'last_name' END,
                        CASE WHEN hire_date IS NULL THEN 'hire_date' END,
                        CASE WHEN termination_date IS NOT NULL AND termination_date < hire_date THEN 'termination_date' END,
                        CASE WHEN license_number IS NULL THEN 'license_number' END,
                        CASE WHEN license_state IS NULL THEN 'license_state' END,
                        CASE WHEN date_of_birth IS NULL THEN 'date_of_birth' END,
                        CASE WHEN home_terminal IS NULL THEN 'home_terminal' END,
                        CASE WHEN employment_status IS NULL THEN 'employment_status' END,
                        CASE WHEN cdl_class IS NULL THEN 'cdl_class' END,
                        CASE WHEN years_experience IS NULL OR years_experience < 0 THEN 'years_experience' END,
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
        FROM stg_drivers;

        INSERT INTO dq.quarantine_drivers (
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
        FROM stg_drivers
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        INSERT INTO silver.drivers AS t (
            driver_id,
            first_name,
            last_name,
            hire_date,
            termination_date,
            license_number,
            license_state,
            date_of_birth,
            home_terminal,
            employment_status,
            cdl_class,
            years_experience,
            loaded_at,
            silver_batch_id
        )
        SELECT DISTINCT ON (driver_id)
            driver_id,
            first_name,
            last_name,
            hire_date,
            termination_date,
            license_number,
            license_state,
            date_of_birth,
            home_terminal,
            employment_status,
            cdl_class,
            years_experience,
            loaded_at,
            p_batch_id
        FROM stg_drivers
        WHERE reject_reason IS NULL
        ORDER BY
            driver_id,
            loaded_at DESC
        ON CONFLICT (driver_id) DO UPDATE
        SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            hire_date = EXCLUDED.hire_date,
            termination_date = EXCLUDED.termination_date,
            license_number = EXCLUDED.license_number,
            license_state = EXCLUDED.license_state,
            date_of_birth = EXCLUDED.date_of_birth,
            home_terminal = EXCLUDED.home_terminal,
            employment_status = EXCLUDED.employment_status,
            cdl_class = EXCLUDED.cdl_class,
            years_experience = EXCLUDED.years_experience,
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

GRANT ALL ON silver.drivers TO etl_writer;
GRANT SELECT ON silver.drivers TO dq_runner;
GRANT ALL ON dq.quarantine_drivers TO etl_writer;
GRANT SELECT ON dq.quarantine_drivers TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_drivers_id_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_drivers(TEXT) TO etl_writer;

COMMIT;
