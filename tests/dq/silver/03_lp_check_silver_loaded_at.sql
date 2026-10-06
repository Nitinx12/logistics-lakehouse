-- Checks silver loaded_at has no nulls and nothing from the future.
DO $$
DECLARE
    tbl RECORD;
    null_count BIGINT;
    future_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver loaded_at';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'silver'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs')
        ORDER BY table_name
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM silver.%I WHERE loaded_at IS NULL',
            tbl.table_name
        )
        INTO null_count;

        EXECUTE format(
            'SELECT COUNT(*) FROM silver.%I WHERE loaded_at > NOW() + INTERVAL ''5 minutes''',
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
        RAISE EXCEPTION 'Silver loaded_at validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Silver tables have a valid loaded_at.';
    END IF;
END $$;
