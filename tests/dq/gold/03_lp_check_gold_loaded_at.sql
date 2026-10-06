-- Checks gold loaded_at has no nulls and nothing from the future.
DO $$
DECLARE
    tbl RECORD;
    null_count BIGINT;
    future_count BIGINT;
    has_column BOOLEAN;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold loaded_at';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'gold'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs')
        ORDER BY table_name
    LOOP
        SELECT COUNT(*) > 0
        INTO has_column
        FROM information_schema.columns
        WHERE table_schema = 'gold'
            AND table_name = tbl.table_name
            AND column_name = 'loaded_at';

        IF NOT has_column THEN
            RAISE NOTICE '  - Table: % | no loaded_at, skipped.', tbl.table_name;
            CONTINUE;
        END IF;

        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I WHERE loaded_at IS NULL',
            tbl.table_name
        )
        INTO null_count;

        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I WHERE loaded_at > NOW() + INTERVAL ''5 minutes''',
            tbl.table_name
        )
        INTO future_count;

        IF null_count > 0 OR future_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s (%s null, %s future); ',
                tbl.table_name,
                null_count,
                future_count
            );
            RAISE NOTICE '  X Table: % | null: % | future: %.', tbl.table_name, null_count, future_count;
        ELSE
            RAISE NOTICE '  OK Table: % | null: 0 | future: 0.', tbl.table_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'loaded_at Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold loaded_at validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Gold tables have a valid loaded_at.';
    END IF;
END $$;
