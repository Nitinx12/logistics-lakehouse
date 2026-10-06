/*
================================================================================
Procedure : silver.load_routes
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            fully reload route reference data from bronze into silver.

Source    : bronze.routes
Target    : silver.routes
Quarantine: dq.quarantine_routes

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read all bronze records and dedupe by route_id, latest loaded_at wins.
5. Standardize text fields and safely cast integers and numerics.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Truncate silver.routes and reload all valid rows in one transaction.
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
Routes is a tiny reference table (58 rows), so a full reload is
cheaper than watermark tracking. Rerunning a batch yields the same state.
Invalid records are stored in dq.quarantine_routes.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS silver.routes (
    route_id VARCHAR PRIMARY KEY,
    origin_city VARCHAR NOT NULL,
    origin_state VARCHAR NOT NULL,
    destination_city VARCHAR NOT NULL,
    destination_state VARCHAR NOT NULL,
    typical_distance_miles BIGINT NOT NULL,
    base_rate_per_mile DOUBLE PRECISION NOT NULL,
    fuel_surcharge_rate DOUBLE PRECISION NOT NULL,
    typical_transit_days BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_routes_amounts
    CHECK (
        typical_distance_miles > 0
        AND base_rate_per_mile > 0
        AND fuel_surcharge_rate >= 0
        AND typical_transit_days > 0
    )
);

CREATE INDEX IF NOT EXISTS idx_silver_routes_loaded_at
ON silver.routes (loaded_at DESC);

CREATE TABLE IF NOT EXISTS dq.quarantine_routes (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_routes_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_routes(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_routes';
    c_target CONSTANT VARCHAR := 'silver.routes';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_routes', 0);
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

        CREATE TEMP TABLE stg_routes ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY route_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.routes
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.route_id) AS route_id,
                INITCAP(silver.clean_text(b.origin_city)) AS origin_city,
                UPPER(silver.clean_text(b.origin_state)) AS origin_state,
                INITCAP(silver.clean_text(b.destination_city))
                    AS destination_city,
                UPPER(silver.clean_text(b.destination_state))
                    AS destination_state,
                silver.try_int(b.typical_distance_miles)::BIGINT
                    AS typical_distance_miles,
                silver.try_numeric(b.base_rate_per_mile)::DOUBLE PRECISION
                    AS base_rate_per_mile,
                silver.try_numeric(b.fuel_surcharge_rate)::DOUBLE PRECISION
                    AS fuel_surcharge_rate,
                silver.try_int(b.typical_transit_days)::BIGINT
                    AS typical_transit_days,
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
                        CASE WHEN route_id IS NULL THEN 'route_id' END,
                        CASE WHEN origin_city IS NULL THEN 'origin_city' END,
                        CASE WHEN origin_state IS NULL THEN 'origin_state' END,
                        CASE WHEN destination_city IS NULL
                            THEN 'destination_city' END,
                        CASE WHEN destination_state IS NULL
                            THEN 'destination_state' END,
                        CASE WHEN typical_distance_miles IS NULL
                            OR typical_distance_miles <= 0
                            THEN 'typical_distance_miles' END,
                        CASE WHEN base_rate_per_mile IS NULL
                            OR base_rate_per_mile <= 0
                            THEN 'base_rate_per_mile' END,
                        CASE WHEN fuel_surcharge_rate IS NULL
                            OR fuel_surcharge_rate < 0
                            THEN 'fuel_surcharge_rate' END,
                        CASE WHEN typical_transit_days IS NULL
                            OR typical_transit_days <= 0
                            THEN 'typical_transit_days' END,
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
        FROM stg_routes;

        INSERT INTO dq.quarantine_routes (
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
        FROM stg_routes
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        TRUNCATE TABLE silver.routes;

        INSERT INTO silver.routes (
            route_id,
            origin_city,
            origin_state,
            destination_city,
            destination_state,
            typical_distance_miles,
            base_rate_per_mile,
            fuel_surcharge_rate,
            typical_transit_days,
            loaded_at,
            silver_batch_id
        )
        SELECT DISTINCT ON (route_id)
            route_id,
            origin_city,
            origin_state,
            destination_city,
            destination_state,
            typical_distance_miles,
            base_rate_per_mile,
            fuel_surcharge_rate,
            typical_transit_days,
            loaded_at,
            p_batch_id
        FROM stg_routes
        WHERE reject_reason IS NULL
        ORDER BY
            route_id,
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

GRANT ALL ON silver.routes TO etl_writer;
GRANT SELECT ON silver.routes TO dq_runner;
GRANT ALL ON dq.quarantine_routes TO etl_writer;
GRANT SELECT ON dq.quarantine_routes TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_routes_id_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_routes(TEXT) TO etl_writer;

COMMIT;
