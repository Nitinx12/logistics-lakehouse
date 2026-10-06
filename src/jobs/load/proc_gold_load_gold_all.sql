/*
================================================================================
Procedure : gold.load_gold_all
Purpose   : Run every gold loader in FK safe order with a single batch id,
            so one call refreshes dimensions first, then facts.

Source    : silver.* via gold dims
Target    : gold.dim_* then gold.fact_*

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Call the date dimension, then the six entity dimensions.
5. Call the fact loaders with degenerate keys already resolved.
6. Mark the orchestrator run SUCCESS with the per step row counts.
7. Commit the transaction and release the advisory lock.

Error Handling
--------------
- Each per table procedure commits on its own, so tables loaded before a
  failure keep their data and can be inspected from gold.etl_logs.
- The child calls run outside any exception block on purpose: wrapping them
  in BEGIN ... EXCEPTION would open a subtransaction, and PostgreSQL forbids
  the child COMMIT statements inside one (invalid transaction termination).
- When a step fails its error propagates to the caller, later steps are
  skipped, and the orchestrator row stays STARTED with the last completed
  step recorded. The next run marks it FAILED through orphan cleanup.
- Callers must use a short lived connection, so the advisory lock is
  released on disconnect when a run aborts before the final unlock.

Parameters
----------
p_batch_id : TEXT
    Unique identifier for the current ETL batch, shared by every step.

Returns
-------
None

Concurrency
-----------
Uses a PostgreSQL advisory lock to ensure only one instance of this
procedure runs at a time. Per table procedures hold their own locks too.

Load Strategy
-------------
Orchestrator only. It calls per table dimension and fact loaders in
FK safe order and never touches table data itself.

================================================================================
*/

BEGIN;

CREATE OR REPLACE PROCEDURE gold.load_gold_all(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'gold.load_gold_all';
    c_target CONSTANT VARCHAR := 'gold.gold_all';
    c_lock_key CONSTANT BIGINT := hashtextextended('gold.load_gold_all', 0);
    v_log_id BIGINT;
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

    CALL gold.load_dim_date(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_dim_date'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_dim_customers(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_dim_customers'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_dim_drivers(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_dim_drivers'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_dim_facilities(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_dim_facilities'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_dim_routes(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_dim_routes'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_dim_trailers(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_dim_trailers'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_dim_trucks(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_dim_trucks'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_fact_loads(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_fact_loads'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_fact_trips(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_fact_trips'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_fact_fuel_purchases(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_fact_fuel_purchases'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_fact_delivery_events(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_fact_delivery_events'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_fact_maintenance(p_batch_id);

    UPDATE gold.etl_logs
    SET error_message = 'completed: gold.load_fact_maintenance'
    WHERE id = v_log_id;

    COMMIT;

    CALL gold.load_fact_safety_incidents(p_batch_id);

    UPDATE gold.etl_logs
    SET
        status = 'SUCCESS',
        error_message = NULL,
        rows_in = (
            SELECT COALESCE(SUM(rows_in), 0)
            FROM gold.etl_logs
            WHERE batch_id = p_batch_id
                AND procedure_name IN (
                    'gold.load_dim_date',
                    'gold.load_dim_customers',
                    'gold.load_dim_drivers',
                    'gold.load_dim_facilities',
                    'gold.load_dim_routes',
                    'gold.load_dim_trailers',
                    'gold.load_dim_trucks',
                    'gold.load_fact_loads',
                    'gold.load_fact_trips',
                    'gold.load_fact_fuel_purchases',
                    'gold.load_fact_delivery_events',
                    'gold.load_fact_maintenance',
                    'gold.load_fact_safety_incidents'
                )
                AND status = 'SUCCESS'
        ),
        rows_out = (
            SELECT COALESCE(SUM(rows_out), 0)
            FROM gold.etl_logs
            WHERE batch_id = p_batch_id
                AND procedure_name IN (
                    'gold.load_dim_date',
                    'gold.load_dim_customers',
                    'gold.load_dim_drivers',
                    'gold.load_dim_facilities',
                    'gold.load_dim_routes',
                    'gold.load_dim_trailers',
                    'gold.load_dim_trucks',
                    'gold.load_fact_loads',
                    'gold.load_fact_trips',
                    'gold.load_fact_fuel_purchases',
                    'gold.load_fact_delivery_events',
                    'gold.load_fact_maintenance',
                    'gold.load_fact_safety_incidents'
                )
                AND status = 'SUCCESS'
        ),
        rows_rejected = (
            SELECT COALESCE(SUM(rows_rejected), 0)
            FROM gold.etl_logs
            WHERE batch_id = p_batch_id
                AND procedure_name IN (
                    'gold.load_dim_date',
                    'gold.load_dim_customers',
                    'gold.load_dim_drivers',
                    'gold.load_dim_facilities',
                    'gold.load_dim_routes',
                    'gold.load_dim_trailers',
                    'gold.load_dim_trucks',
                    'gold.load_fact_loads',
                    'gold.load_fact_trips',
                    'gold.load_fact_fuel_purchases',
                    'gold.load_fact_delivery_events',
                    'gold.load_fact_maintenance',
                    'gold.load_fact_safety_incidents'
                )
                AND status = 'SUCCESS'
        ),
        finished_at = NOW()
    WHERE id = v_log_id;

    COMMIT;

    PERFORM pg_advisory_unlock(c_lock_key);
END;
$$;

GRANT EXECUTE ON PROCEDURE gold.load_gold_all(TEXT) TO etl_writer;

COMMIT;
