-- Checks every fact surrogate key resolves to a dimension row.
DO $$
DECLARE
    cfg RECORD;
    orphan_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Dimension Keys';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('fact_loads', 'customer_sk', 'dim_customer', 'customer_sk'),
            ('fact_loads', 'route_sk', 'dim_route', 'route_sk'),
            ('fact_trips', 'driver_sk', 'dim_driver', 'driver_sk'),
            ('fact_trips', 'truck_sk', 'dim_truck', 'truck_sk'),
            ('fact_trips', 'trailer_sk', 'dim_trailer', 'trailer_sk'),
            ('fact_fuel_purchases', 'truck_sk', 'dim_truck', 'truck_sk'),
            ('fact_fuel_purchases', 'driver_sk', 'dim_driver', 'driver_sk'),
            ('fact_delivery_events', 'facility_sk', 'dim_facility', 'facility_sk'),
            ('fact_maintenance', 'truck_sk', 'dim_truck', 'truck_sk'),
            ('fact_safety_incidents', 'truck_sk', 'dim_truck', 'truck_sk'),
            ('fact_safety_incidents', 'driver_sk', 'dim_driver', 'driver_sk')
        ) AS v(fact_table, fact_col, dim_table, dim_col)
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I AS f LEFT JOIN gold.%I AS d ON d.%I = f.%I WHERE d.%I IS NULL',
            cfg.fact_table,
            cfg.dim_table,
            cfg.dim_col,
            cfg.fact_col,
            cfg.dim_col
        )
        INTO orphan_count;

        IF orphan_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s.%s (%s orphans); ',
                cfg.fact_table,
                cfg.fact_col,
                orphan_count
            );
            RAISE NOTICE '  X %.% | orphans: %.', cfg.fact_table, cfg.fact_col, orphan_count;
        ELSE
            RAISE NOTICE '  OK %.% | resolves.', cfg.fact_table, cfg.fact_col;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Dimension Key Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold dimension key validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every Gold fact key resolves to a dimension.';
    END IF;
END $$;
