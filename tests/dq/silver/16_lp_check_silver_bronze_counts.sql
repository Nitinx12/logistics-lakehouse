-- Checks silver row counts reconcile exactly to bronze per table.
DO $$
DECLARE
    tbl RECORD;
    bronze_count BIGINT;
    silver_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Bronze Reconciliation';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'silver'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs')
        ORDER BY table_name
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM bronze.%I', tbl.table_name)
        INTO bronze_count;

        EXECUTE format('SELECT COUNT(*) FROM silver.%I', tbl.table_name)
        INTO silver_count;

        IF bronze_count <> silver_count THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s (bronze %s, silver %s); ',
                tbl.table_name,
                bronze_count,
                silver_count
            );
            RAISE NOTICE '  X Table: % | bronze: % | silver: %.', tbl.table_name, bronze_count, silver_count;
        ELSE
            RAISE NOTICE '  OK Table: % | counts match: %.', tbl.table_name, silver_count;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Reconciliation Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Silver reconciliation validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Silver reconciles to Bronze on every table.';
    END IF;
END $$;
