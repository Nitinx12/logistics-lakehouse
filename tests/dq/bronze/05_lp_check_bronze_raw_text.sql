-- Checks bronze stores raw text and leaves typing to silver.
DO $$
DECLARE
    tbl RECORD;
    bad_count BIGINT;
    bad_cols TEXT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Bronze Raw Text Columns';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'bronze'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs', 'etl_watermarks')
        ORDER BY table_name
    LOOP
        SELECT
            COUNT(*),
            STRING_AGG(column_name || ' ' || data_type, ', ' ORDER BY column_name)
        INTO bad_count, bad_cols
        FROM information_schema.columns
        WHERE table_schema = 'bronze'
            AND table_name = tbl.table_name
            AND column_name <> '_loaded_at'
            AND data_type <> 'text';

        IF bad_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format('%s (%s); ', tbl.table_name, bad_cols);
            RAISE NOTICE '  X Table: % | typed columns: %.', tbl.table_name, bad_cols;
        ELSE
            RAISE NOTICE '  OK Table: % | all source columns are text.', tbl.table_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Raw Text Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Bronze raw text validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Bronze tables land source columns as text.';
    END IF;
END $$;
