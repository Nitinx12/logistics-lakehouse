-- Checks key and name text carries no leading or trailing whitespace.
DO $$
DECLARE
    cfg RECORD;
    bad_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Trimmed Text';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('customers', 'customer_id'),
            ('customers', 'customer_name'),
            ('drivers', 'driver_id'),
            ('facilities', 'facility_id'),
            ('facilities', 'facility_name'),
            ('routes', 'route_id'),
            ('trailers', 'trailer_id'),
            ('trucks', 'truck_id'),
            ('loads', 'load_id'),
            ('trips', 'trip_id'),
            ('fuel_purchases', 'fuel_purchase_id'),
            ('delivery_events', 'event_id'),
            ('maintenance_records', 'maintenance_id'),
            ('safety_incidents', 'incident_id')
        ) AS v(table_name, column_name)
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM silver.%I WHERE %I <> BTRIM(%I)',
            cfg.table_name,
            cfg.column_name,
            cfg.column_name
        )
        INTO bad_count;

        IF bad_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s.%s (%s untrimmed); ',
                cfg.table_name,
                cfg.column_name,
                bad_count
            );
            RAISE NOTICE '  X %.% | untrimmed: %.', cfg.table_name, cfg.column_name, bad_count;
        ELSE
            RAISE NOTICE '  OK %.% | trimmed.', cfg.table_name, cfg.column_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Trim Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Silver trim validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Silver text is trimmed.';
    END IF;
END $$;
