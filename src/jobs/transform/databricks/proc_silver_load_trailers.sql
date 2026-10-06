/*
================================================================================
Procedure : silver.load_trailers
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            incrementally upsert trailer data from bronze into silver.

Source    : bronze.trailers
Target    : silver.trailers
Quarantine: dq.quarantine_trailers

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated bronze records using the loaded_at watermark.
5. Standardize text fields and safely cast dates and integers.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Deduplicate records by trailer_id, keeping the latest loaded_at record.
9. Incrementally upsert valid records into silver.trailers.
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
Invalid records are stored in dq.quarantine_trailers.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS silver.trailers (
    trailer_id VARCHAR PRIMARY KEY,
    trailer_number BIGINT NOT NULL,
    trailer_type VARCHAR NOT NULL,
    length_feet BIGINT NOT NULL,
    model_year BIGINT NOT NULL,
    vin VARCHAR NOT NULL,
    acquisition_date DATE NOT NULL,
    status VARCHAR NOT NULL,
    current_location VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_trailers_amounts
    CHECK (
        trailer_number > 0
        AND length_feet > 0
        AND model_year > 0
    )
);

CREATE INDEX IF NOT EXISTS idx_silver_trailers_loaded_at
ON silver.trailers (loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_trailers_status
ON silver.trailers (status);

CREATE TABLE IF NOT EXISTS dq.quarantine_trailers (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_trailers_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_trailers(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_trailers';
    c_target CONSTANT VARCHAR := 'silver.trailers';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_trailers', 0);
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

        CREATE TEMP TABLE stg_trailers ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY trailer_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.trailers
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.trailer_id) AS trailer_id,
                silver.try_int(b.trailer_number)::BIGINT AS trailer_number,
                UPPER(silver.clean_text(b.trailer_type)) AS trailer_type,
                silver.try_int(b.length_feet)::BIGINT AS length_feet,
                silver.try_int(b.model_year)::BIGINT AS model_year,
                silver.clean_text(b.vin) AS vin,
                silver.try_timestamp(b.acquisition_date)::DATE
                    AS acquisition_date,
                UPPER(silver.clean_text(b.status)) AS status,
                INITCAP(silver.clean_text(b.current_location))
                    AS current_location,
                l.loaded_ts AS loaded_at
            FROM deduplicated AS b
            CROSS JOIN LATERAL (
                SELECT silver.try_timestamptz(b.loaded_at) AS loaded_ts
            ) AS l
            WHERE b.rnk = 1
                AND (
                    l.loaded_ts IS NULL
                    OR l.loaded_ts >= COALESCE(
                        (SELECT MAX(s.loaded_at) FROM silver.trailers AS s),
                        '-infinity'::TIMESTAMPTZ
                    )
                )
        )
        SELECT
            cleaned.*,
            NULLIF(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN trailer_id IS NULL THEN 'trailer_id' END,
                        CASE WHEN trailer_number IS NULL OR trailer_number <= 0
                            THEN 'trailer_number' END,
                        CASE WHEN trailer_type IS NULL THEN 'trailer_type' END,
                        CASE WHEN length_feet IS NULL OR length_feet <= 0
                            THEN 'length_feet' END,
                        CASE WHEN model_year IS NULL OR model_year <= 0
                            THEN 'model_year' END,
                        CASE WHEN vin IS NULL THEN 'vin' END,
                        CASE WHEN acquisition_date IS NULL
                            THEN 'acquisition_date' END,
                        CASE WHEN status IS NULL THEN 'status' END,
                        CASE WHEN current_location IS NULL
                            THEN 'current_location' END,
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
        FROM stg_trailers;

        INSERT INTO dq.quarantine_trailers (
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
        FROM stg_trailers
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        INSERT INTO silver.trailers AS t (
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
            silver_batch_id
        )
        SELECT DISTINCT ON (trailer_id)
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
        FROM stg_trailers
        WHERE reject_reason IS NULL
        ORDER BY
            trailer_id,
            loaded_at DESC
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

GRANT ALL ON silver.trailers TO etl_writer;
GRANT SELECT ON silver.trailers TO dq_runner;
GRANT ALL ON dq.quarantine_trailers TO etl_writer;
GRANT SELECT ON dq.quarantine_trailers TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_trailers_id_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_trailers(TEXT) TO etl_writer;

COMMIT;
