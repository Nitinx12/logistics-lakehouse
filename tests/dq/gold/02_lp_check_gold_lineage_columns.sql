-- Checks every gold table carries batch lineage and loaded_at where defined.
DO $$
DECLARE
    tbl RECORD;
    missing TEXT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Lineage Columns';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'gold'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs')
        ORDER BY table_name
    LOOP
        SELECT STRING_AGG(col, ', ' ORDER BY col)
        INTO missing
        FROM (
            VALUES ('gold_batch_id')
        ) AS need(col)
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns AS c
            WHERE c.table_schema = 'gold'
                AND c.table_name = tbl.table_name
                AND c.column_name = need.col
        );

        IF missing IS NOT NULL THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format('%s (missing %s); ', tbl.table_name, missing);
            RAISE NOTICE '  X Table: % | missing: %.', tbl.table_name, missing;
        ELSE
            RAISE NOTICE '  OK Table: % | lineage columns present.', tbl.table_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Lineage Column Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold lineage column validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Gold tables carry lineage columns.';
    END IF;
END $$;
