/*
================================================================================
Procedure : gold.load_dim_trucks
Purpose   : Incrementally upsert truck dimension rows from silver,
            keeping stable surrogate keys and the unknown member.

Source    : silver.trucks
Target    : gold.dim_truck

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Ensure the unknown member row (truck_sk = -1) exists.
5. Read new and updated silver rows using the loaded_at watermark.
6. Upsert rows on the business key, overwriting attributes (SCD Type 1).
7. Existing surrogate keys never change; new keys take the next value.
8. Record rows_in, rows_out, execution status, and errors.
9. Commit the transaction and release the advisory lock.

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
Incremental SCD Type 1 upsert driven by the loaded_at watermark.
Rerunning a batch yields the same state.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS gold.dim_truck (
    truck_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    truck_id VARCHAR NOT NULL,
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
    gold_batch_id TEXT,
    gold_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    gold_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_truck_business_key UNIQUE (truck_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_truck_loaded_at
ON gold.dim_truck (loaded_at DESC);

CREATE OR REPLACE PROCEDURE gold.load_dim_trucks(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'gold.load_dim_trucks';
    c_target CONSTANT VARCHAR := 'gold.dim_truck';
    c_lock_key CONSTANT BIGINT := hashtextextended('gold.load_dim_trucks', 0);
    v_log_id BIGINT;
    v_rows_in BIGINT := 0;
    v_rows_out BIGINT := 0;
    v_error TEXT;
    v_sqlstate TEXT;
BEGIN
    IF NOT pg_try_advisory_lock(c_lock_key) THEN
        RAISE EXCEPTION '% is already running', c_procedure
            USING ERRCODE = '55P03';
    END IF;

    UPDATE gold.etl_logs
    SET
        status = 'FAILED',
        error_message = 'Orphaned run: session ended before completion',
        finished_at = NOW()
    WHERE procedure_name = c_procedure
        AND target_table = c_target
        AND status = 'STARTED';

    INSERT INTO gold.etl_logs (
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

        INSERT INTO gold.dim_truck (
            truck_sk,
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
            gold_batch_id
        )
        OVERRIDING SYSTEM VALUE
        VALUES (
            -1,
            'UNKNOWN',
            0,
            'Unknown',
            0,
            'UNKNOWN',
            '1900-01-01',
            0,
            'UNKNOWN',
            0,
            'UNKNOWN',
            'Unknown',
            '1900-01-01 00:00:00+00',
            p_batch_id
        )
        ON CONFLICT (truck_sk) DO NOTHING;

        CREATE TEMP TABLE stg_dim_truck ON COMMIT DROP AS
        SELECT
            s.truck_id,
            s.unit_number,
            s.make,
            s.model_year,
            s.vin,
            s.acquisition_date,
            s.acquisition_mileage,
            s.fuel_type,
            s.tank_capacity_gallons,
            s.status,
            s.home_terminal,
            s.loaded_at
        FROM silver.trucks AS s
        WHERE s.loaded_at >= COALESCE(
            (
                SELECT MAX(d.loaded_at)
                FROM gold.dim_truck AS d
                WHERE d.truck_sk <> -1
            ),
            '-infinity'::TIMESTAMPTZ
        );

        SELECT COUNT(*)
        INTO v_rows_in
        FROM stg_dim_truck;

        INSERT INTO gold.dim_truck AS t (
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
            gold_batch_id
        )
        SELECT
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
        FROM stg_dim_truck
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
            gold_batch_id = EXCLUDED.gold_batch_id,
            gold_updated_at = NOW()
        WHERE t.loaded_at < EXCLUDED.loaded_at;

        GET DIAGNOSTICS v_rows_out = ROW_COUNT;

        UPDATE gold.etl_logs
        SET
            status = 'SUCCESS',
            rows_in = v_rows_in,
            rows_out = v_rows_out,
            finished_at = NOW()
        WHERE id = v_log_id;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error = MESSAGE_TEXT,
                v_sqlstate = RETURNED_SQLSTATE;
    END;

    IF v_error IS NOT NULL THEN
        UPDATE gold.etl_logs
        SET
            status = 'FAILED',
            rows_in = v_rows_in,
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

GRANT ALL ON gold.dim_truck TO etl_writer;
GRANT SELECT ON gold.dim_truck TO dq_runner;
GRANT SELECT ON gold.dim_truck TO analyst_ro;
GRANT SELECT ON gold.dim_truck TO dashboard_ro;
GRANT USAGE, SELECT ON SEQUENCE gold.dim_truck_truck_sk_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE gold.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE gold.load_dim_trucks(TEXT) TO etl_writer;

COMMIT;
