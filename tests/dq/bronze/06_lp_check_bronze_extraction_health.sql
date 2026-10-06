-- Checks every bronze extraction finished SUCCESS with no run left STARTED.
DO $$
DECLARE
    tbl RECORD;
    success_count BIGINT;
    started_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Bronze Extraction Health';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'bronze'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs', 'etl_watermarks')
        ORDER BY table_name
    LOOP
        SELECT COUNT(*)
        INTO success_count
        FROM bronze.etl_logs
        WHERE target_table = tbl.table_name
            AND status = 'SUCCESS';

        SELECT COUNT(*)
        INTO started_count
        FROM bronze.etl_logs
        WHERE target_table = tbl.table_name
            AND status = 'STARTED';

        IF success_count = 0 OR started_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s (%s success, %s started); ',
                tbl.table_name,
                success_count,
                started_count
            );
            RAISE NOTICE '  X Table: % | success: % | started: %.', tbl.table_name, success_count, started_count;
        ELSE
            RAISE NOTICE '  OK Table: % | success: %.', tbl.table_name, success_count;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Extraction Health Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Bronze extraction health validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every Bronze extraction is healthy.';
    END IF;
END $$;
