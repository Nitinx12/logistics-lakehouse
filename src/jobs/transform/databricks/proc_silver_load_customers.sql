/*
================================================================================
Procedure : silver.load_customers
Purpose   : Clean, validate, deduplicate, quarantine invalid records, and
            incrementally upsert customer data from bronze into silver.

Source    : bronze.customers
Target    : silver.customers
Quarantine: dq.quarantine_customers

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Read new and updated bronze records using the loaded_at watermark.
5. Standardize text fields and safely cast dates and integers.
6. Validate mandatory fields and business rules.
7. Write invalid records to the quarantine table with their rejection reason.
8. Deduplicate records by customer_id, keeping the latest loaded_at record.
9. Incrementally upsert valid records into silver.customers.
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
Invalid records are stored in dq.quarantine_customers.

================================================================================
*/

BEGIN;

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_name VARCHAR NOT NULL,
    customer_type VARCHAR NOT NULL,
    credit_terms_days BIGINT NOT NULL,
    primary_freight_type VARCHAR NOT NULL,
    account_status VARCHAR NOT NULL,
    contract_start_date DATE NOT NULL,
    annual_revenue_potential BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    silver_batch_id TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_customers_amounts
    CHECK (
        credit_terms_days >= 0
        AND annual_revenue_potential >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_silver_customers_loaded_at
ON silver.customers (loaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_customers_account_status
ON silver.customers (account_status);

CREATE TABLE IF NOT EXISTS dq.quarantine_customers (
    id BIGSERIAL PRIMARY KEY,
    batch_id TEXT,
    row_hash TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    source_row JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dq_quarantine_customers_row_hash UNIQUE (row_hash)
);

CREATE OR REPLACE PROCEDURE silver.load_customers(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_customers';
    c_target CONSTANT VARCHAR := 'silver.customers';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_customers', 0);
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

        CREATE TEMP TABLE stg_customers ON COMMIT DROP AS
        WITH deduplicated AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id ORDER BY loaded_at DESC
                ) AS rnk
            FROM bronze.customers
        ),
        cleaned AS (
            SELECT
                TO_JSONB(b) - 'rnk' AS source_row,
                silver.clean_text(b.customer_id) AS customer_id,
                silver.clean_text(b.customer_name) AS customer_name,
                UPPER(silver.clean_text(b.customer_type)) AS customer_type,
                silver.try_int(b.credit_terms_days)::BIGINT AS credit_terms_days,
                UPPER(silver.clean_text(b.primary_freight_type)) AS primary_freight_type,
                UPPER(silver.clean_text(b.account_status)) AS account_status,
                silver.try_timestamp(b.contract_start_date)::DATE AS contract_start_date,
                silver.try_int(b.annual_revenue_potential)::BIGINT AS annual_revenue_potential,
                l.loaded_ts AS loaded_at
            FROM deduplicated AS b
            CROSS JOIN LATERAL (
                SELECT silver.try_timestamptz(b.loaded_at) AS loaded_ts
            ) AS l
            WHERE b.rnk = 1
                AND (
                    l.loaded_ts IS NULL
                    OR l.loaded_ts >= COALESCE(
                        (SELECT MAX(s.loaded_at) FROM silver.customers AS s),
                        '-infinity'::TIMESTAMPTZ
                    )
                )
        )
        SELECT
            cleaned.*,
            NULLIF(
                ARRAY_TO_STRING(
                    ARRAY[
                        CASE WHEN customer_id IS NULL THEN 'customer_id' END,
                        CASE WHEN customer_name IS NULL THEN 'customer_name' END,
                        CASE WHEN customer_type IS NULL THEN 'customer_type' END,
                        CASE WHEN credit_terms_days IS NULL OR credit_terms_days < 0 THEN 'credit_terms_days' END,
                        CASE WHEN primary_freight_type IS NULL THEN 'primary_freight_type' END,
                        CASE WHEN account_status IS NULL THEN 'account_status' END,
                        CASE WHEN contract_start_date IS NULL THEN 'contract_start_date' END,
                        CASE WHEN annual_revenue_potential IS NULL OR annual_revenue_potential < 0 THEN 'annual_revenue_potential' END,
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
        FROM stg_customers;

        INSERT INTO dq.quarantine_customers (
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
        FROM stg_customers
        WHERE reject_reason IS NOT NULL
        ON CONFLICT (row_hash) DO NOTHING;

        INSERT INTO silver.customers AS t (
            customer_id,
            customer_name,
            customer_type,
            credit_terms_days,
            primary_freight_type,
            account_status,
            contract_start_date,
            annual_revenue_potential,
            loaded_at,
            silver_batch_id
        )
        SELECT DISTINCT ON (customer_id)
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
        FROM stg_customers
        WHERE reject_reason IS NULL
        ORDER BY
            customer_id,
            loaded_at DESC
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

GRANT ALL ON silver.customers TO etl_writer;
GRANT SELECT ON silver.customers TO dq_runner;
GRANT ALL ON dq.quarantine_customers TO etl_writer;
GRANT SELECT ON dq.quarantine_customers TO dq_runner;
GRANT USAGE, SELECT ON SEQUENCE dq.quarantine_customers_id_seq TO etl_writer;
GRANT USAGE, SELECT ON SEQUENCE silver.etl_logs_id_seq TO etl_writer;
GRANT EXECUTE ON PROCEDURE silver.load_customers(TEXT) TO etl_writer;

COMMIT;
