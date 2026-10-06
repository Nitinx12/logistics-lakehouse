-- Checks every gold table exists and holds rows.
DO $$
DECLARE
    tbl RECORD;
    row_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Tables';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'gold'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs')
        ORDER BY table_name
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I',
            tbl.table_name
        )
        INTO row_count;

        IF row_count = 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format('%s (0 rows); ', tbl.table_name);
            RAISE NOTICE '  X Table: % | empty.', tbl.table_name;
        ELSE
            RAISE NOTICE '  OK Table: % | rows: %.', tbl.table_name, row_count;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Gold Table Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold table validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Gold tables contain data.';
    END IF;
END $$;
