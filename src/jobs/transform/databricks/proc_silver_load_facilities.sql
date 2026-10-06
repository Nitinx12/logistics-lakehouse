/*
================================================================================
Procedure : silver.load_facilities
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            fully reload facility reference data from bronze into silver.

Source    : bronze.facilities
Target    : silver.facilities
Quarantine: dq.quarantine_facilities

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read all bronze records and dedupe by facility_id, latest loaded_at wins.
5. Standardize text fields and safely cast doubles, integers, timestamps.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Truncate silver.facilities and reload all valid rows in one transaction.
9. Record rows_in, rows_out, rows_rejected, execution status, and errors.
10. Commit the transaction and release the advisory lock.

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
Full reload via truncate and reload (staging swap in one transaction).
Facilities is a tiny reference table (50 rows), so a full reload is
cheaper than watermark tracking. Rerunning a batch yields the same state.
Invalid records are stored in dq.quarantine_facilities.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS silver.facilities (
    facility_id VARCHAR PRIMARY KEY,
    facility_name VARCHAR NOT NULL,
    facility_type VARCHAR NOT NULL,
    city VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    dock_doors BIGINT NOT NULL,
    operating_hours VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_facilities_coords
    CHECK (
        latitude BETWEEN -90 AND 90
        AND longitude BETWEEN -180 AND 180
        AND dock_doors >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_silver_facilities_loaded_at
ON silver.facilities (loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_facilities_facility_type
ON silver.facilities (facility_type);

CREATE TABLE IF NOT EXISTS dq.quarantine_facilities (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_facilities_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_facilities(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_facilities';
    c_target CONSTANT VARCHAR := 'silver.facilities';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_facilities', 0);
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

        CREATE TEMP TABLE stg_facilities ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY facility_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.facilities
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.facility_id) AS facility_id,
                silver.clean_text(b.facility_name) AS facility_name,
                UPPER(silver.clean_text(b.facility_type)) AS facility_type,
                INITCAP(silver.clean_text(b.city)) AS city,
                UPPER(silver.clean_text(b.state)) AS state,
                silver.try_numeric(b.latitude)::DOUBLE PRECISION AS latitude,
                silver.try_numeric(b.longitude)::DOUBLE PRECISION AS longitude,
                silver.try_int(b.dock_doors)::BIGINT AS dock_doors,
                UPPER(silver.clean_text(b.operating_hours)) AS operating_hours,
                l.loaded_ts AS loaded_at
            FROM deduplicated AS b
            CROSS JOIN LATERAL (
                SELECT silver.try_timestamptz(b.loaded_at) AS loaded_ts
            ) AS l
            WHERE b.rnk = 1
        )
        SELECT
            cleaned.*,
            NULLIF(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN facility_id IS NULL THEN 'facility_id' END,
                        CASE WHEN facility_name IS NULL THEN 'facility_name' END,
                        CASE WHEN facility_type IS NULL THEN 'facility_type' END,
                        CASE WHEN city IS NULL THEN 'city' END,
                        CASE WHEN state IS NULL THEN 'state' END,
                        CASE WHEN latitude IS NULL THEN 'latitude' END,
                        CASE WHEN latitude NOT BETWEEN -90 AND 90
                            THEN 'latitude_range' END,
                        CASE WHEN longitude IS NULL THEN 'longitude' END,
                        CASE WHEN longitude NOT BETWEEN -180 AND 180
                            THEN 'longitude_range' END,
                        CASE WHEN dock_doors IS NULL OR dock_doors < 0 THEN 'dock_doors' END,
                        CASE WHEN operating_hours IS NULL THEN 'operating_hours' END,
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
        FROM stg_facilities;

        INSERT INTO dq.quarantine_facilities (
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
        FROM stg_facilities
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        TRUNCATE TABLE silver.facilities;

        INSERT INTO silver.facilities (
            facility_id,
            facility_name,
            facility_type,
            city,
            state,
            latitude,
            longitude,
            dock_doors,
            operating_hours,
            loaded_at,
            silver_batch_id
        )
        SELECT DISTINCT ON (facility_id)
            facility_id,
            facility_name,
            facility_type,
            city,
            state,
            latitude,
            longitude,
            dock_doors,
            operating_hours,
            loaded_at,
            p_batch_id
        FROM stg_facilities
        WHERE reject_reason IS NULL
        ORDER BY
            facility_id,
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

GRANT ALL ON silver.facilities TO etl_writer;
GRANT SELECT ON silver.facilities TO dq_runner;
GRANT ALL ON dq.quarantine_facilities TO etl_writer;
GRANT SELECT ON dq.quarantine_facilities TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_facilities_id_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_facilities(TEXT) TO etl_writer;

COMMIT;
