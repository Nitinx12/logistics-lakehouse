-- Checks every silver row carries its source batch id.
DO $$
DECLARE
    tbl RECORD;
    null_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Batch Lineage';
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
            'SELECT COUNT(*) FROM silver.%I WHERE silver_batch_id IS NULL',
            tbl.table_name
        )
        INTO null_count;

        IF null_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format('%s (%s rows); ', tbl.table_name, null_count);
            RAISE NOTICE '  X Table: % | missing batch id: %.', tbl.table_name, null_count;
        ELSE
            RAISE NOTICE '  OK Table: % | batch lineage complete.', tbl.table_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Batch Lineage Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Silver batch lineage validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every Silver row traces back to a batch.';
    END IF;
END $$;
