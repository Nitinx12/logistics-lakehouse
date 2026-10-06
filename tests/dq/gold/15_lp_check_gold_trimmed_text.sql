-- Checks key text carries no leading or trailing whitespace.
DO $$
DECLARE
    cfg RECORD;
    bad_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Trimmed Text';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('dim_customer', 'customer_id'),
            ('dim_driver', 'driver_id'),
            ('dim_facility', 'facility_id'),
            ('dim_route', 'route_id'),
            ('dim_trailer', 'trailer_id'),
            ('dim_truck', 'truck_id'),
            ('fact_loads', 'load_id'),
            ('fact_trips', 'trip_id'),
            ('fact_fuel_purchases', 'fuel_purchase_id'),
            ('fact_delivery_events', 'event_id'),
            ('fact_maintenance', 'maintenance_id'),
            ('fact_safety_incidents', 'incident_id')
        ) AS v(table_name, column_name)
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I WHERE %I <> BTRIM(%I)',
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
        RAISE EXCEPTION 'Gold trim validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Gold text is trimmed.';
    END IF;
END $$;
