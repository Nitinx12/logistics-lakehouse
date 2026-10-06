/*
================================================================================
Procedure : gold.load_dim_customers
Purpose   : Incrementally upsert customer dimension rows from silver,
            keeping stable surrogate keys and the unknown member.

Source    : silver.customers
Target    : gold.dim_customer

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Ensure the unknown member row (customer_sk = -1) exists.
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

CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    customer_name VARCHAR NOT NULL,
    customer_type VARCHAR NOT NULL,
    credit_terms_days BIGINT NOT NULL,
    primary_freight_type VARCHAR NOT NULL,
    account_status VARCHAR NOT NULL,
    contract_start_date DATE NOT NULL,
    annual_revenue_potential BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    gold_batch_id TEXT,
    gold_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    gold_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_customer_business_key UNIQUE (customer_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_loaded_at
ON gold.dim_customer (loaded_at DESC);

CREATE OR REPLACE PROCEDURE gold.load_dim_customers(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'gold.load_dim_customers';
    c_target CONSTANT VARCHAR := 'gold.dim_customer';
    c_lock_key CONSTANT BIGINT := hashtextextended('gold.load_dim_customers', 0);
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

        INSERT INTO gold.dim_customer (
            customer_sk,
            customer_id,
            customer_name,
            customer_type,
            credit_terms_days,
            primary_freight_type,
            account_status,
            contract_start_date,
            annual_revenue_potential,
            loaded_at,
            gold_batch_id
        )
        OVERRIDING SYSTEM VALUE
        VALUES (
            -1,
            'UNKNOWN',
            'Unknown',
            'UNKNOWN',
            0,
            'UNKNOWN',
            'UNKNOWN',
            '1900-01-01',
            0,
            '1900-01-01 00:00:00+00',
            p_batch_id
        )
        ON CONFLICT (customer_sk) DO NOTHING;

        CREATE TEMP TABLE stg_dim_customer ON COMMIT DROP AS
        SELECT
            s.customer_id,
            s.customer_name,
            s.customer_type,
            s.credit_terms_days,
            s.primary_freight_type,
            s.account_status,
            s.contract_start_date,
            s.annual_revenue_potential,
            s.loaded_at
        FROM silver.customers AS s
        WHERE s.loaded_at >= COALESCE(
            (
                SELECT MAX(d.loaded_at)
                FROM gold.dim_customer AS d
                WHERE d.customer_sk <> -1
            ),
            '-infinity'::TIMESTAMPTZ
        );

        SELECT COUNT(*)
        INTO v_rows_in
        FROM stg_dim_customer;

        INSERT INTO gold.dim_customer AS t (
            customer_id,
            customer_name,
            customer_type,
            credit_terms_days,
            primary_freight_type,
            account_status,
            contract_start_date,
            annual_revenue_potential,
            loaded_at,
            gold_batch_id
        )
        SELECT
            customer_id,
            customer_name,
            customer_type,
            credit_terms_days,
            primary_freight_type,
            account_status,
            contract_start_date,
            annual_revenue_potential,
            loaded_at,
            p_batch_id
        FROM stg_dim_customer
        ON CONFLICT (customer_id) DO UPDATE
        SET
            customer_name = EXCLUDED.customer_name,
            customer_type = EXCLUDED.customer_type,
            credit_terms_days = EXCLUDED.credit_terms_days,
            primary_freight_type = EXCLUDED.primary_freight_type,
            account_status = EXCLUDED.account_status,
            contract_start_date = EXCLUDED.contract_start_date,
            annual_revenue_potential = EXCLUDED.annual_revenue_potential,
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

GRANT ALL ON gold.dim_customer TO etl_writer;
GRANT SELECT ON gold.dim_customer TO dq_runner;
GRANT SELECT ON gold.dim_customer TO analyst_ro;
GRANT USAGE, SELECT ON SEQUENCE gold.dim_customer_customer_sk_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE gold.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE gold.load_dim_customers(TEXT) TO etl_writer;

COMMIT;
