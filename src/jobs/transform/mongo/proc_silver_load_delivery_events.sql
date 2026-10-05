/*
================================================================================
Procedure : silver.load_delivery_events
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            incrementally upsert delivery event data from bronze into silver.

Source    : bronze.delivery_events
Target    : silver.delivery_events
Quarantine: dq.quarantine_delivery_events

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated bronze records using the loaded_at watermark.
5. Standardize text fields and safely cast timestamps, integers, and Booleans.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Deduplicate records by event_id, keeping the latest loaded_at record.
9. Calculate delay_minutes from scheduled_datetime and actual_datetime.
10. Incrementally upsert valid records into silver.delivery_events.
11. Update an existing record only when the incoming loaded_at is newer.
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
Incremental upsert (SCD Type 1) driven by the loaded_at watermark.
Existing records are updated only when the incoming loaded_at is newer.
Invalid records are stored in dq.quarantine_delivery_events.

================================================================================
*/

BEGIN;

CREATE OR REPLACE FUNCTION silver.clean_text(p_value TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN LOWER(BTRIM(p_value)) IN ('', 'null', 'none', 'nan', 'n/a', 'nil') THEN NULL
        ELSE REGEXP_REPLACE(BTRIM(p_value), '\s+', ' ', 'g')
    END;
$$;

CREATE OR REPLACE FUNCTION silver.to_bool(p_value TEXT)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN LOWER(BTRIM(p_value)) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
        WHEN LOWER(BTRIM(p_value)) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
    END;
$$;

CREATE OR REPLACE FUNCTION silver.try_int(p_value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
BEGIN
    IF p_value IS NULL OR p_value !~ '^[+-]?\d+(\.\d+)?$' THEN
        RETURN NULL;
    END IF;
    RETURN ROUND(p_value::NUMERIC)::INTEGER;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION silver.try_timestamp(p_value TEXT)
RETURNS TIMESTAMP
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $$
BEGIN
    IF p_value IS NULL OR p_value !~ '^\d{4}-\d{2}-\d{2}' THEN
        RETURN NULL;
    END IF;
    RETURN p_value::TIMESTAMP;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION silver.try_timestamptz(p_value TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $$
BEGIN
    IF p_value IS NULL OR p_value !~ '^\d{4}-\d{2}-\d{2}' THEN
        RETURN NULL;
    END IF;
    RETURN p_value::TIMESTAMPTZ;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;

CREATE TABLE IF NOT EXISTS silver.delivery_events (
    event_id VARCHAR PRIMARY KEY,
    load_id VARCHAR NOT NULL,
    trip_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    facility_id VARCHAR NOT NULL,
    scheduled_datetime TIMESTAMP NOT NULL,
    actual_datetime TIMESTAMP NOT NULL,
    delay_minutes INTEGER NOT NULL,
    detention_minutes INTEGER NOT NULL,
    on_time_flag BOOLEAN NOT NULL,
    location_city VARCHAR NOT NULL,
    location_state VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_delivery_events_detention
        CHECK (detention_minutes >= 0)
);

CREATE INDEX IF NOT EXISTS idx_silver_delivery_events_loaded_at
    ON silver.delivery_events (loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_delivery_events_load_id
    ON silver.delivery_events (load_id);

CREATE INDEX IF NOT EXISTS idx_silver_delivery_events_trip_id
    ON silver.delivery_events (trip_id);

CREATE INDEX IF NOT EXISTS idx_silver_delivery_events_facility_id
    ON silver.delivery_events (facility_id);

CREATE TABLE IF NOT EXISTS dq.quarantine_delivery_events (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_delivery_events_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_delivery_events(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_delivery_events';
    c_target CONSTANT VARCHAR := 'silver.delivery_events';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_delivery_events', 0);
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

        CREATE TEMP TABLE stg_delivery_events ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.delivery_events
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.event_id) AS event_id,
                silver.clean_text(b.load_id) AS load_id,
                silver.clean_text(b.trip_id) AS trip_id,
                UPPER(silver.clean_text(b.event_type)) AS event_type,
                silver.clean_text(b.facility_id) AS facility_id,
                silver.try_timestamp(b.scheduled_datetime) AS scheduled_datetime,
                silver.try_timestamp(b.actual_datetime) AS actual_datetime,
                silver.try_int(b.detention_minutes) AS detention_minutes,
                silver.to_bool(b.on_time_flag) AS on_time_flag,
                INITCAP(silver.clean_text(b.location_city)) AS location_city,
                UPPER(silver.clean_text(b.location_state)) AS location_state,
                l.loaded_ts AS loaded_at
            FROM deduplicated AS b
            CROSS JOIN LATERAL (
                SELECT silver.try_timestamptz(b.loaded_at) AS loaded_ts
            ) AS l
            WHERE b.rnk = 1
                AND (
                    l.loaded_ts IS NULL
                    OR l.loaded_ts >= COALESCE(
                        (SELECT MAX(s.loaded_at) FROM silver.delivery_events AS s),
                        '-infinity'::TIMESTAMPTZ
                    )
                )
        )
        SELECT
            cleaned.*,
            NULLIF(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN event_id IS NULL THEN 'event_id' END,
                        CASE WHEN load_id IS NULL THEN 'load_id' END,
                        CASE WHEN trip_id IS NULL THEN 'trip_id' END,
                        CASE WHEN event_type IS NULL THEN 'event_type' END,
                        CASE WHEN facility_id IS NULL THEN 'facility_id' END,
                        CASE WHEN scheduled_datetime IS NULL THEN 'scheduled_datetime' END,
                        CASE WHEN actual_datetime IS NULL THEN 'actual_datetime' END,
                        CASE WHEN detention_minutes IS NULL OR detention_minutes < 0 THEN 'detention_minutes' END,
                        CASE WHEN on_time_flag IS NULL THEN 'on_time_flag' END,
                        CASE WHEN location_city IS NULL THEN 'location_city' END,
                        CASE WHEN location_state IS NULL THEN 'location_state' END,
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
        FROM stg_delivery_events;

        INSERT INTO dq.quarantine_delivery_events (
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
        FROM stg_delivery_events
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        INSERT INTO silver.delivery_events AS t (
            event_id,
            load_id,
            trip_id,
            event_type,
            facility_id,
            scheduled_datetime,
            actual_datetime,
            delay_minutes,
            detention_minutes,
            on_time_flag,
            location_city,
            location_state,
            loaded_at,
            silver_batch_id
        )
        SELECT DISTINCT ON (event_id)
            event_id,
            load_id,
            trip_id,
            event_type,
            facility_id,
            scheduled_datetime,
            actual_datetime,
            ROUND(EXTRACT(EPOCH FROM (actual_datetime - scheduled_datetime)) / 60)::INTEGER,
            detention_minutes,
            on_time_flag,
            location_city,
            location_state,
            loaded_at,
            p_batch_id
        FROM stg_delivery_events
        WHERE reject_reason IS NULL
        ORDER BY
            event_id,
            loaded_at DESC
        ON CONFLICT (event_id) DO UPDATE
        SET
            load_id = EXCLUDED.load_id,
            trip_id = EXCLUDED.trip_id,
            event_type = EXCLUDED.event_type,
            facility_id = EXCLUDED.facility_id,
            scheduled_datetime = EXCLUDED.scheduled_datetime,
            actual_datetime = EXCLUDED.actual_datetime,
            delay_minutes = EXCLUDED.delay_minutes,
            detention_minutes = EXCLUDED.detention_minutes,
            on_time_flag = EXCLUDED.on_time_flag,
            location_city = EXCLUDED.location_city,
            location_state = EXCLUDED.location_state,
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

GRANT ALL ON silver.delivery_events TO etl_writer;
GRANT SELECT ON silver.delivery_events TO dq_runner;
GRANT ALL ON dq.quarantine_delivery_events TO etl_writer;
GRANT SELECT ON dq.quarantine_delivery_events TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_delivery_events_id_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_delivery_events(TEXT) TO etl_writer;

COMMIT;
