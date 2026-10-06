/*
================================================================================
Procedure : silver.load_loads
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            incrementally upsert load data from bronze into silver.

Source    : bronze.loads
Target    : silver.loads
Quarantine: dq.quarantine_loads

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated bronze records using the loaded_at watermark.
5. Standardize text fields and safely cast dates and numerics.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Deduplicate records by load_id, keeping the latest loaded_at record.
9. Incrementally upsert valid records into silver.loads.
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
Invalid records are stored in dq.quarantine_loads.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS silver.loads (
    load_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    route_id VARCHAR NOT NULL,
    load_date DATE NOT NULL,
    load_type VARCHAR NOT NULL,
    weight_lbs BIGINT NOT NULL,
    pieces BIGINT NOT NULL,
    revenue DOUBLE PRECISION NOT NULL,
    fuel_surcharge DOUBLE PRECISION NOT NULL,
    accessorial_charges BIGINT NOT NULL,
    load_status VARCHAR NOT NULL,
    booking_type VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_loads_amounts
    CHECK (
        weight_lbs > 0
        AND pieces > 0
        AND revenue >= 0
        AND fuel_surcharge >= 0
        AND accessorial_charges >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_silver_loads_loaded_at
ON silver.loads (loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_loads_customer_id
ON silver.loads (customer_id);

CREATE INDEX IF NOT EXISTS idx_silver_loads_route_id
ON silver.loads (route_id);

CREATE INDEX IF NOT EXISTS idx_silver_loads_load_date
ON silver.loads (load_date);

CREATE TABLE IF NOT EXISTS dq.quarantine_loads (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_loads_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_loads(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_loads';
    c_target CONSTANT VARCHAR := 'silver.loads';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_loads', 0);
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

        CREATE TEMP TABLE stg_loads ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY load_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.loads
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.load_id) AS load_id,
                silver.clean_text(b.customer_id) AS customer_id,
                silver.clean_text(b.route_id) AS route_id,
                silver.try_timestamp(b.load_date)::DATE AS load_date,
                UPPER(silver.clean_text(b.load_type)) AS load_type,
                silver.try_int(b.weight_lbs)::BIGINT AS weight_lbs,
                silver.try_int(b.pieces)::BIGINT AS pieces,
                silver.try_numeric(b.revenue)::DOUBLE PRECISION AS revenue,
                silver.try_numeric(b.fuel_surcharge)::DOUBLE PRECISION
                    AS fuel_surcharge,
                silver.try_int(b.accessorial_charges)::BIGINT
                    AS accessorial_charges,
                UPPER(silver.clean_text(b.load_status)) AS load_status,
                UPPER(silver.clean_text(b.booking_type)) AS booking_type,
                l.loaded_ts AS loaded_at
            FROM deduplicated AS b
            CROSS JOIN LATERAL (
                SELECT silver.try_timestamptz(b.loaded_at) AS loaded_ts
            ) AS l
            WHERE b.rnk = 1
                AND (
                    l.loaded_ts IS NULL
                    OR l.loaded_ts >= COALESCE(
                        (SELECT MAX(s.loaded_at) FROM silver.loads AS s),
                        '-infinity'::TIMESTAMPTZ
                    )
                )
        )
        SELECT
            cleaned.*,
            NULLIF(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN load_id IS NULL THEN 'load_id' END,
                        CASE WHEN customer_id IS NULL THEN 'customer_id' END,
                        CASE WHEN route_id IS NULL THEN 'route_id' END,
                        CASE WHEN load_date IS NULL THEN 'load_date' END,
                        CASE WHEN load_type IS NULL THEN 'load_type' END,
                        CASE WHEN weight_lbs IS NULL OR weight_lbs <= 0
                            THEN 'weight_lbs' END,
                        CASE WHEN pieces IS NULL OR pieces <= 0 THEN 'pieces' END,
                        CASE WHEN revenue IS NULL OR revenue < 0 THEN 'revenue' END,
                        CASE WHEN fuel_surcharge IS NULL OR fuel_surcharge < 0
                            THEN 'fuel_surcharge' END,
                        CASE WHEN accessorial_charges IS NULL
                            OR accessorial_charges < 0
                            THEN 'accessorial_charges' END,
                        CASE WHEN load_status IS NULL THEN 'load_status' END,
                        CASE WHEN booking_type IS NULL THEN 'booking_type' END,
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
        FROM stg_loads;

        INSERT INTO dq.quarantine_loads (
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
        FROM stg_loads
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        INSERT INTO silver.loads AS t (
            load_id,
            customer_id,
            route_id,
            load_date,
            load_type,
            weight_lbs,
            pieces,
            revenue,
            fuel_surcharge,
            accessorial_charges,
            load_status,
            booking_type,
            loaded_at,
            silver_batch_id
        )
        SELECT DISTINCT ON (load_id)
            load_id,
            customer_id,
            route_id,
            load_date,
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
        FROM stg_loads
        WHERE reject_reason IS NULL
        ORDER BY
            load_id,
            loaded_at DESC
        ON CONFLICT (load_id) DO UPDATE
        SET
            customer_id = EXCLUDED.customer_id,
            route_id = EXCLUDED.route_id,
            load_date = EXCLUDED.load_date,
            load_type = EXCLUDED.load_type,
            weight_lbs = EXCLUDED.weight_lbs,
            pieces = EXCLUDED.pieces,
            revenue = EXCLUDED.revenue,
            fuel_surcharge = EXCLUDED.fuel_surcharge,
            accessorial_charges = EXCLUDED.accessorial_charges,
            load_status = EXCLUDED.load_status,
            booking_type = EXCLUDED.booking_type,
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

GRANT ALL ON silver.loads TO etl_writer;
GRANT SELECT ON silver.loads TO dq_runner;
GRANT ALL ON dq.quarantine_loads TO etl_writer;
GRANT SELECT ON dq.quarantine_loads TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_loads_id_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_loads(TEXT) TO etl_writer;

COMMIT;
