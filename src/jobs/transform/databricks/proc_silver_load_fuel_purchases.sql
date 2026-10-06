/*
================================================================================
Procedure : silver.load_fuel_purchases
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            incrementally upsert fuel purchase data from bronze into silver.

Source    : bronze.fuel_purchases
Target    : silver.fuel_purchases
Quarantine: dq.quarantine_fuel_purchases

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated bronze records using the loaded_at watermark.
5. Standardize text fields and safely cast dates and numerics.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Deduplicate records by fuel_purchase_id, keeping the latest loaded_at.
9. Incrementally upsert valid records into silver.fuel_purchases.
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
Invalid records are stored in dq.quarantine_fuel_purchases.

Notes
-----
truck_id and driver_id are nullable by design (about 2 percent of rows
lack one: fuel card not assigned to a trip asset). Nulls there are kept,
never quarantined; gold resolves misses to the unknown member.
purchase_date accepts ISO timestamps and YYYYMMDD integers.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS silver.fuel_purchases (
    fuel_purchase_id VARCHAR PRIMARY KEY,
    trip_id VARCHAR NOT NULL,
    truck_id VARCHAR,
    driver_id VARCHAR,
    purchase_date TIMESTAMP NOT NULL,
    location_city VARCHAR NOT NULL,
    location_state VARCHAR NOT NULL,
    gallons DOUBLE PRECISION NOT NULL,
    price_per_gallon DOUBLE PRECISION NOT NULL,
    total_cost DOUBLE PRECISION NOT NULL,
    fuel_card_number VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_fuel_purchases_amounts
    CHECK (
        gallons > 0
        AND price_per_gallon > 0
        AND total_cost >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_silver_fuel_purchases_loaded_at
ON silver.fuel_purchases (loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_fuel_purchases_trip_id
ON silver.fuel_purchases (trip_id);

CREATE INDEX IF NOT EXISTS idx_silver_fuel_purchases_truck_id
ON silver.fuel_purchases (truck_id);

CREATE INDEX IF NOT EXISTS idx_silver_fuel_purchases_purchase_date
ON silver.fuel_purchases (purchase_date);

CREATE TABLE IF NOT EXISTS dq.quarantine_fuel_purchases (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_fuel_purchases_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_fuel_purchases(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_fuel_purchases';
    c_target CONSTANT VARCHAR := 'silver.fuel_purchases';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_fuel_purchases', 0);
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

        CREATE TEMP TABLE stg_fuel_purchases ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY fuel_purchase_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.fuel_purchases
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.fuel_purchase_id) AS fuel_purchase_id,
                silver.clean_text(b.trip_id) AS trip_id,
                silver.clean_text(b.truck_id) AS truck_id,
                silver.clean_text(b.driver_id) AS driver_id,
                CASE
                    WHEN b.purchase_date ~ '^[0-9]{8}$'
                        THEN TO_DATE(b.purchase_date, 'YYYYMMDD')::TIMESTAMP
                    ELSE silver.try_timestamp(b.purchase_date)
                END AS purchase_date,
                INITCAP(silver.clean_text(b.location_city)) AS location_city,
                UPPER(silver.clean_text(b.location_state)) AS location_state,
                silver.try_numeric(b.gallons)::DOUBLE PRECISION AS gallons,
                silver.try_numeric(b.price_per_gallon)::DOUBLE PRECISION
                    AS price_per_gallon,
                silver.try_numeric(b.total_cost)::DOUBLE PRECISION AS total_cost,
                silver.clean_text(b.fuel_card_number) AS fuel_card_number,
                l.loaded_ts AS loaded_at
            FROM deduplicated AS b
            CROSS JOIN LATERAL (
                SELECT silver.try_timestamptz(b.loaded_at) AS loaded_ts
            ) AS l
            WHERE b.rnk = 1
                AND (
                    l.loaded_ts IS NULL
                    OR l.loaded_ts >= COALESCE(
                        (SELECT MAX(s.loaded_at) FROM silver.fuel_purchases AS s),
                        '-infinity'::TIMESTAMPTZ
                    )
                )
        )
        SELECT
            cleaned.*,
            NULLIF(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN fuel_purchase_id IS NULL THEN 'fuel_purchase_id' END,
                        CASE WHEN trip_id IS NULL THEN 'trip_id' END,
                        CASE WHEN purchase_date IS NULL THEN 'purchase_date' END,
                        CASE WHEN location_city IS NULL THEN 'location_city' END,
                        CASE WHEN location_state IS NULL THEN 'location_state' END,
                        CASE WHEN gallons IS NULL OR gallons <= 0 THEN 'gallons' END,
                        CASE WHEN price_per_gallon IS NULL OR price_per_gallon <= 0
                            THEN 'price_per_gallon' END,
                        CASE WHEN total_cost IS NULL OR total_cost < 0 THEN 'total_cost' END,
                        CASE WHEN total_cost IS NOT NULL AND gallons IS NOT NULL
                            AND price_per_gallon IS NOT NULL
                            AND ABS(total_cost - gallons * price_per_gallon)
                                > GREATEST(1.0, total_cost * 0.02)
                            THEN 'total_cost_mismatch' END,
                        CASE WHEN fuel_card_number IS NULL THEN 'fuel_card_number' END,
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
        FROM stg_fuel_purchases;

        INSERT INTO dq.quarantine_fuel_purchases (
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
        FROM stg_fuel_purchases
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        INSERT INTO silver.fuel_purchases AS t (
            fuel_purchase_id,
            trip_id,
            truck_id,
            driver_id,
            purchase_date,
            location_city,
            location_state,
            gallons,
            price_per_gallon,
            total_cost,
            fuel_card_number,
            loaded_at,
            silver_batch_id
        )
        SELECT DISTINCT ON (fuel_purchase_id)
            fuel_purchase_id,
            trip_id,
            truck_id,
            driver_id,
            purchase_date,
            location_city,
            location_state,
            gallons,
            price_per_gallon,
            total_cost,
            fuel_card_number,
            loaded_at,
            p_batch_id
        FROM stg_fuel_purchases
        WHERE reject_reason IS NULL
        ORDER BY
            fuel_purchase_id,
            loaded_at DESC
        ON CONFLICT (fuel_purchase_id) DO UPDATE
        SET
            trip_id = EXCLUDED.trip_id,
            truck_id = EXCLUDED.truck_id,
            driver_id = EXCLUDED.driver_id,
            purchase_date = EXCLUDED.purchase_date,
            location_city = EXCLUDED.location_city,
            location_state = EXCLUDED.location_state,
            gallons = EXCLUDED.gallons,
            price_per_gallon = EXCLUDED.price_per_gallon,
            total_cost = EXCLUDED.total_cost,
            fuel_card_number = EXCLUDED.fuel_card_number,
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

GRANT ALL ON silver.fuel_purchases TO etl_writer;
GRANT SELECT ON silver.fuel_purchases TO dq_runner;
GRANT ALL ON dq.quarantine_fuel_purchases TO etl_writer;
GRANT SELECT ON dq.quarantine_fuel_purchases TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_fuel_purchases_id_seq
TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_fuel_purchases(TEXT) TO etl_writer;

COMMIT;
