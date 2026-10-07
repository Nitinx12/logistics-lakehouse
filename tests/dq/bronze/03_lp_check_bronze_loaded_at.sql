-- Checks every bronze table has loaded_at with no null or unparseable values.
DO $$
DECLARE
    tbl RECORD;
    null_count BIGINT;
    max_loaded TIMESTAMPTZ;
    has_column BOOLEAN;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Bronze loaded_at';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'bronze'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs', 'etl_watermarks', 'delivery_events_stream')
        ORDER BY table_name
    LOOP
        SELECT COUNT(*) > 0
        INTO has_column
        FROM information_schema.columns
        WHERE table_schema = 'bronze'
            AND table_name = tbl.table_name
            AND column_name = 'loaded_at';

        IF NOT has_column THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format('%s (missing loaded_at); ', tbl.table_name);
            RAISE NOTICE '  X Table: % | loaded_at column missing.', tbl.table_name;
            CONTINUE;
        END IF;

        EXECUTE format(
            'SELECT COUNT(*) FROM bronze.%I WHERE loaded_at IS NULL',
            tbl.table_name
        )
        INTO null_count;

        IF null_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format('%s (%s null loaded_at); ', tbl.table_name, null_count);
            RAISE NOTICE '  X Table: % | null loaded_at: %.', tbl.table_name, null_count;
            CONTINUE;
        END IF;

        BEGIN
            EXECUTE format(
                'SELECT MAX(loaded_at::TIMESTAMPTZ) FROM bronze.%I',
                tbl.table_name
            )
            INTO max_loaded;
        EXCEPTION
            WHEN OTHERS THEN
                any_failed := TRUE;
                fail_msg := fail_msg || format('%s (unparseable loaded_at); ', tbl.table_name);
                RAISE NOTICE '  X Table: % | loaded_at has unparseable values.', tbl.table_name;
                CONTINUE;
        END;

        RAISE NOTICE '  OK Table: % | nulls: 0 | max: %.', tbl.table_name, max_loaded;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'loaded_at Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Bronze loaded_at validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Bronze tables have a valid loaded_at.';
    END IF;
END $$;
