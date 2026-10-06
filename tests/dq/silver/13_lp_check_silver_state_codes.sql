-- Checks state codes are two letter uppercase throughout silver.
DO $$
DECLARE
    cfg RECORD;
    bad_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver State Codes';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('facilities', 'state'),
            ('routes', 'origin_state'),
            ('routes', 'destination_state'),
            ('fuel_purchases', 'location_state'),
            ('delivery_events', 'location_state'),
            ('safety_incidents', 'location_state')
        ) AS v(table_name, column_name)
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM silver.%I WHERE LENGTH(%I) <> 2 OR %I <> UPPER(%I)',
            cfg.table_name,
            cfg.column_name,
            cfg.column_name,
            cfg.column_name
        )
        INTO bad_count;

        IF bad_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s.%s (%s bad); ',
                cfg.table_name,
                cfg.column_name,
                bad_count
            );
            RAISE NOTICE '  X %.% | bad codes: %.', cfg.table_name, cfg.column_name, bad_count;
        ELSE
            RAISE NOTICE '  OK %.% | codes standard.', cfg.table_name, cfg.column_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'State Code Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Silver state code validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Silver state codes are standard.';
    END IF;
END $$;
