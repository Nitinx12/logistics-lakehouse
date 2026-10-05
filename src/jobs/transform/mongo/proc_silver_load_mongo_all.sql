/*
================================================================================
Procedure : silver.load_mongo_all
Purpose   : Run every Mongo sourced silver loader in sequence with a single
            batch id, so one call refreshes the whole mongo layer.

Source    : bronze.delivery_events, bronze.maintenance_records,
            bronze.safety_incidents
Target    : silver.delivery_events, silver.maintenance_records,
            silver.safety_incidents

Process
-------
1. Acquire an advisory lock to prevent concurrent executions.
2. Mark orphaned STARTED ETL log records as FAILED.
3. Create an ETL audit log entry for the current batch and commit it.
4. Call silver.load_delivery_events with the same batch id.
5. Call silver.load_maintenance_records with the same batch id.
6. Call silver.load_safety_incidents with the same batch id.
7. Mark the orchestrator run SUCCESS with the per step row counts.
8. Commit the transaction and release the advisory lock.

Error Handling
--------------
- Each per table procedure commits on its own, so tables loaded before a
  failure keep their data and can be inspected from silver.etl_logs.
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
Orchestrator only. It calls per table incremental loaders and never
touches table data itself.

================================================================================
*/

BEGIN;

CREATE OR REPLACE PROCEDURE silver.load_mongo_all(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    c_procedure CONSTANT VARCHAR := 'silver.load_mongo_all';
    c_target CONSTANT VARCHAR := 'silver.mongo_all';
    c_lock_key CONSTANT BIGINT := hashtextextended('silver.load_mongo_all', 0);
    v_log_id BIGINT;
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

    CALL silver.load_delivery_events(p_batch_id);

    UPDATE silver.etl_logs
    SET error_message = 'completed: silver.load_delivery_events'
    WHERE id = v_log_id;

    COMMIT;

    CALL silver.load_maintenance_records(p_batch_id);

    UPDATE silver.etl_logs
    SET error_message = 'completed: silver.load_delivery_events, silver.load_maintenance_records'
    WHERE id = v_log_id;

    COMMIT;

    CALL silver.load_safety_incidents(p_batch_id);

    UPDATE silver.etl_logs
    SET
        status = 'SUCCESS',
        error_message = NULL,
        rows_in = (
            SELECT COALESCE(SUM(rows_in), 0)
            FROM silver.etl_logs
            WHERE batch_id = p_batch_id
                AND procedure_name IN (
                    'silver.load_delivery_events',
                    'silver.load_maintenance_records',
                    'silver.load_safety_incidents'
                )
                AND status = 'SUCCESS'
        ),
        rows_out = (
            SELECT COALESCE(SUM(rows_out), 0)
            FROM silver.etl_logs
            WHERE batch_id = p_batch_id
                AND procedure_name IN (
                    'silver.load_delivery_events',
                    'silver.load_maintenance_records',
                    'silver.load_safety_incidents'
                )
                AND status = 'SUCCESS'
        ),
        rows_rejected = (
            SELECT COALESCE(SUM(rows_rejected), 0)
            FROM silver.etl_logs
            WHERE batch_id = p_batch_id
                AND procedure_name IN (
                    'silver.load_delivery_events',
                    'silver.load_maintenance_records',
                    'silver.load_safety_incidents'
                )
                AND status = 'SUCCESS'
        ),
        finished_at = NOW()
    WHERE id = v_log_id;

    COMMIT;

    PERFORM pg_advisory_unlock(c_lock_key);
END;
$$;

GRANT EXECUTE ON PROCEDURE silver.load_mongo_all(TEXT) TO etl_writer;

COMMIT;
