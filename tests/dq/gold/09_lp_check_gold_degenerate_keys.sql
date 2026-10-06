-- Checks degenerate keys link facts to their parent facts.
DO $$
DECLARE
    cfg RECORD;
    orphan_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Degenerate Keys';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('fact_trips', 'load_id', 'fact_loads', 'load_id'),
            ('fact_fuel_purchases', 'trip_id', 'fact_trips', 'trip_id'),
            ('fact_delivery_events', 'trip_id', 'fact_trips', 'trip_id'),
            ('fact_delivery_events', 'load_id', 'fact_loads', 'load_id'),
            ('fact_safety_incidents', 'trip_id', 'fact_trips', 'trip_id')
        ) AS v(child_table, child_col, parent_table, parent_col)
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I AS c LEFT JOIN gold.%I AS p ON p.%I = c.%I WHERE p.%I IS NULL',
            cfg.child_table,
            cfg.parent_table,
            cfg.parent_col,
            cfg.child_col,
            cfg.parent_col
        )
        INTO orphan_count;

        IF orphan_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s.%s (%s orphans); ',
                cfg.child_table,
                cfg.child_col,
                orphan_count
            );
            RAISE NOTICE '  X %.% | orphans: %.', cfg.child_table, cfg.child_col, orphan_count;
        ELSE
            RAISE NOTICE '  OK %.% | resolves.', cfg.child_table, cfg.child_col;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Degenerate Key Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold degenerate key validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every Gold degenerate key resolves.';
    END IF;
END $$;
