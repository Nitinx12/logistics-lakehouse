-- Checks the extractor ingestion marker is present and stamped on every row.
DO $$
DECLARE
    tbl RECORD;
    null_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Bronze Ingestion Marker';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'bronze'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs', 'etl_watermarks', 'delivery_events_stream')
        ORDER BY table_name
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM bronze.%I WHERE _loaded_at IS NULL',
            tbl.table_name
        )
        INTO null_count;

        IF null_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format('%s (%s rows); ', tbl.table_name, null_count);
            RAISE NOTICE '  X Table: % | missing marker: %.', tbl.table_name, null_count;
        ELSE
            RAISE NOTICE '  OK Table: % | marker stamped.', tbl.table_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Ingestion Marker Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Bronze ingestion marker validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every Bronze row carries its ingestion marker.';
    END IF;
END $$;
