-- Checks every fact date key exists in the calendar dimension.
DO $$
DECLARE
    cfg RECORD;
    orphan_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Date Keys';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('fact_loads', 'load_date_key'),
            ('fact_trips', 'dispatch_date_key'),
            ('fact_fuel_purchases', 'purchase_date_key'),
            ('fact_maintenance', 'maintenance_date_key'),
            ('fact_safety_incidents', 'incident_date_key')
        ) AS v(table_name, column_name)
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I AS f LEFT JOIN gold.dim_date AS d ON d.date_key = f.%I WHERE d.date_key IS NULL',
            cfg.table_name,
            cfg.column_name
        )
        INTO orphan_count;

        IF orphan_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s.%s (%s orphans); ',
                cfg.table_name,
                cfg.column_name,
                orphan_count
            );
            RAISE NOTICE '  X %.% | orphans: %.', cfg.table_name, cfg.column_name, orphan_count;
        ELSE
            RAISE NOTICE '  OK %.% | resolves.', cfg.table_name, cfg.column_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Date Key Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold date key validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every Gold date key resolves to the calendar.';
    END IF;
END $$;
