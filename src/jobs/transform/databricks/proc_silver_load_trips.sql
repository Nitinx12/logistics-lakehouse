/*
================================================================================
Procedure : silver.load_trips
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            incrementally upsert trip data from bronze into silver.

Source    : bronze.trips
Target    : silver.trips
Quarantine: dq.quarantine_trips

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated bronze records using the loaded_at watermark.
5. Standardize text fields and safely cast dates and numerics.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Deduplicate records by trip_id, keeping the latest loaded_at record.
9. Incrementally upsert valid records into silver.trips.
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
Invalid records are stored in dq.quarantine_trips.

Notes
-----
driver_id, truck_id, and trailer_id are nullable by design (about 2
percent of rows lack an assignment). Nulls there are kept, never
quarantined; gold resolves misses to the unknown member.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS silver.trips (
    trip_id VARCHAR PRIMARY KEY,
    load_id VARCHAR NOT NULL,
    driver_id VARCHAR,
    truck_id VARCHAR,
    trailer_id VARCHAR,
    dispatch_date DATE NOT NULL,
    actual_distance_miles BIGINT NOT NULL,
    actual_duration_hours DOUBLE PRECISION NOT NULL,
    fuel_gallons_used DOUBLE PRECISION NOT NULL,
    average_mpg DOUBLE PRECISION NOT NULL,
    idle_time_hours DOUBLE PRECISION NOT NULL,
    trip_status VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_trips_amounts
    CHECK (
        actual_distance_miles > 0
        AND actual_duration_hours > 0
        AND fuel_gallons_used > 0
        AND average_mpg > 0
        AND idle_time_hours >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_silver_trips_loaded_at
ON silver.trips (loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_trips_load_id
ON silver.trips (load_id);

CREATE INDEX IF NOT EXISTS idx_silver_trips_driver_id
ON silver.trips (driver_id);

CREATE INDEX IF NOT EXISTS idx_silver_trips_dispatch_date
ON silver.trips (dispatch_date);

CREATE TABLE IF NOT EXISTS dq.quarantine_trips (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_trips_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_trips(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_trips';
    c_target CONSTANT VARCHAR := 'silver.trips';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_trips', 0);
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

        CREATE TEMP TABLE stg_trips ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY trip_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.trips
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.trip_id) AS trip_id,
                silver.clean_text(b.load_id) AS load_id,
                silver.clean_text(b.driver_id) AS driver_id,
                silver.clean_text(b.truck_id) AS truck_id,
                silver.clean_text(b.trailer_id) AS trailer_id,
                silver.try_timestamp(b.dispatch_date)::DATE AS dispatch_date,
                silver.try_int(b.actual_distance_miles)::BIGINT
                    AS actual_distance_miles,
                silver.try_numeric(b.actual_duration_hours)::DOUBLE PRECISION
                    AS actual_duration_hours,
                silver.try_numeric(b.fuel_gallons_used)::DOUBLE PRECISION
                    AS fuel_gallons_used,
                silver.try_numeric(b.average_mpg)::DOUBLE PRECISION
                    AS average_mpg,
                silver.try_numeric(b.idle_time_hours)::DOUBLE PRECISION
                    AS idle_time_hours,
                UPPER(silver.clean_text(b.trip_status)) AS trip_status,
                l.loaded_ts AS loaded_at
            FROM deduplicated AS b
            CROSS JOIN LATERAL (
                SELECT silver.try_timestamptz(b.loaded_at) AS loaded_ts
            ) AS l
            WHERE b.rnk = 1
                AND (
                    l.loaded_ts IS NULL
                    OR l.loaded_ts >= COALESCE(
                        (SELECT MAX(s.loaded_at) FROM silver.trips AS s),
                        '-infinity'::TIMESTAMPTZ
                    )
                )
        )
        SELECT
            cleaned.*,
            NULLIF(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN trip_id IS NULL THEN 'trip_id' END,
                        CASE WHEN load_id IS NULL THEN 'load_id' END,
                        CASE WHEN dispatch_date IS NULL THEN 'dispatch_date' END,
                        CASE WHEN actual_distance_miles IS NULL
                            OR actual_distance_miles <= 0
                            THEN 'actual_distance_miles' END,
                        CASE WHEN actual_duration_hours IS NULL
                            OR actual_duration_hours <= 0
                            THEN 'actual_duration_hours' END,
                        CASE WHEN fuel_gallons_used IS NULL
                            OR fuel_gallons_used <= 0
                            THEN 'fuel_gallons_used' END,
                        CASE WHEN average_mpg IS NULL OR average_mpg <= 0
                            THEN 'average_mpg' END,
                        CASE WHEN average_mpg IS NOT NULL
                            AND actual_distance_miles IS NOT NULL
                            AND fuel_gallons_used IS NOT NULL
                            AND ABS(
                                average_mpg - actual_distance_miles
                                    / fuel_gallons_used
                            ) > GREATEST(0.5, average_mpg * 0.1)
                            THEN 'average_mpg_mismatch' END,
                        CASE WHEN idle_time_hours IS NULL OR idle_time_hours < 0
                            THEN 'idle_time_hours' END,
                        CASE WHEN trip_status IS NULL THEN 'trip_status' END,
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
        FROM stg_trips;

        INSERT INTO dq.quarantine_trips (
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
        FROM stg_trips
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        INSERT INTO silver.trips AS t (
            trip_id,
            load_id,
            driver_id,
            truck_id,
            trailer_id,
            dispatch_date,
            actual_distance_miles,
            actual_duration_hours,
            fuel_gallons_used,
            average_mpg,
            idle_time_hours,
            trip_status,
            loaded_at,
            silver_batch_id
        )
        SELECT DISTINCT ON (trip_id)
            trip_id,
            load_id,
            driver_id,
            truck_id,
            trailer_id,
            dispatch_date,
            actual_distance_miles,
            actual_duration_hours,
            fuel_gallons_used,
            average_mpg,
            idle_time_hours,
            trip_status,
            loaded_at,
            p_batch_id
        FROM stg_trips
        WHERE reject_reason IS NULL
        ORDER BY
            trip_id,
            loaded_at DESC
        ON CONFLICT (trip_id) DO UPDATE
        SET
            load_id = EXCLUDED.load_id,
            driver_id = EXCLUDED.driver_id,
            truck_id = EXCLUDED.truck_id,
            trailer_id = EXCLUDED.trailer_id,
            dispatch_date = EXCLUDED.dispatch_date,
            actual_distance_miles = EXCLUDED.actual_distance_miles,
            actual_duration_hours = EXCLUDED.actual_duration_hours,
            fuel_gallons_used = EXCLUDED.fuel_gallons_used,
            average_mpg = EXCLUDED.average_mpg,
            idle_time_hours = EXCLUDED.idle_time_hours,
            trip_status = EXCLUDED.trip_status,
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

GRANT ALL ON silver.trips TO etl_writer;
GRANT SELECT ON silver.trips TO dq_runner;
GRANT ALL ON dq.quarantine_trips TO etl_writer;
GRANT SELECT ON dq.quarantine_trips TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_trips_id_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_trips(TEXT) TO etl_writer;

COMMIT;
