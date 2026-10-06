-- Checks every silver foreign key resolves to its parent.
DO $$
DECLARE
    cfg RECORD;
    orphan_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Foreign Keys';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('loads', 'customer_id', 'customers', 'customer_id'),
            ('loads', 'route_id', 'routes', 'route_id'),
            ('trips', 'load_id', 'loads', 'load_id'),
            ('trips', 'driver_id', 'drivers', 'driver_id'),
            ('trips', 'truck_id', 'trucks', 'truck_id'),
            ('trips', 'trailer_id', 'trailers', 'trailer_id'),
            ('fuel_purchases', 'trip_id', 'trips', 'trip_id'),
            ('fuel_purchases', 'truck_id', 'trucks', 'truck_id'),
            ('fuel_purchases', 'driver_id', 'drivers', 'driver_id'),
            ('delivery_events', 'trip_id', 'trips', 'trip_id'),
            ('delivery_events', 'load_id', 'loads', 'load_id'),
            ('delivery_events', 'facility_id', 'facilities', 'facility_id'),
            ('maintenance_records', 'truck_id', 'trucks', 'truck_id'),
            ('safety_incidents', 'trip_id', 'trips', 'trip_id'),
            ('safety_incidents', 'truck_id', 'trucks', 'truck_id'),
            ('safety_incidents', 'driver_id', 'drivers', 'driver_id')
        ) AS v(child_table, child_col, parent_table, parent_col)
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM silver.%I AS c LEFT JOIN silver.%I AS p ON p.%I = c.%I WHERE c.%I IS NOT NULL AND p.%I IS NULL',
            cfg.child_table,
            cfg.parent_table,
            cfg.parent_col,
            cfg.child_col,
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
    RAISE NOTICE 'Foreign Key Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Silver foreign key validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Silver foreign keys resolve.';
    END IF;
END $$;
