-- Checks serving joins neither drop rows nor fan out on any fact.
DO $$
DECLARE
    cfg RECORD;
    fact_count BIGINT;
    inner_count BIGINT;
    left_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Join Stability';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('fact_loads', 'dim_customer', 'customer_sk', 'customer_sk'),
            ('fact_loads', 'dim_route', 'route_sk', 'route_sk'),
            ('fact_loads', 'dim_date', 'load_date_key', 'date_key'),
            ('fact_trips', 'dim_driver', 'driver_sk', 'driver_sk'),
            ('fact_trips', 'dim_truck', 'truck_sk', 'truck_sk'),
            ('fact_trips', 'dim_trailer', 'trailer_sk', 'trailer_sk'),
            ('fact_trips', 'dim_date', 'dispatch_date_key', 'date_key'),
            ('fact_fuel_purchases', 'dim_truck', 'truck_sk', 'truck_sk'),
            ('fact_fuel_purchases', 'dim_driver', 'driver_sk', 'driver_sk'),
            ('fact_fuel_purchases', 'dim_date', 'purchase_date_key', 'date_key'),
            ('fact_delivery_events', 'dim_facility', 'facility_sk', 'facility_sk'),
            ('fact_maintenance', 'dim_truck', 'truck_sk', 'truck_sk'),
            ('fact_maintenance', 'dim_date', 'maintenance_date_key', 'date_key'),
            ('fact_safety_incidents', 'dim_truck', 'truck_sk', 'truck_sk'),
            ('fact_safety_incidents', 'dim_driver', 'driver_sk', 'driver_sk'),
            ('fact_safety_incidents', 'dim_date', 'incident_date_key', 'date_key')
        ) AS v(fact_table, dim_table, fact_col, dim_col)
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM gold.%I', cfg.fact_table)
        INTO fact_count;

        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I AS f JOIN gold.%I AS d ON d.%I = f.%I',
            cfg.fact_table,
            cfg.dim_table,
            cfg.dim_col,
            cfg.fact_col
        )
        INTO inner_count;

        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I AS f LEFT JOIN gold.%I AS d ON d.%I = f.%I',
            cfg.fact_table,
            cfg.dim_table,
            cfg.dim_col,
            cfg.fact_col
        )
        INTO left_count;

        IF inner_count <> fact_count OR left_count <> fact_count THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s x %s (fact %s, inner %s, left %s); ',
                cfg.fact_table,
                cfg.dim_table,
                fact_count,
                inner_count,
                left_count
            );
            RAISE NOTICE '  X % x % | fact: % | inner: % | left: %.', cfg.fact_table, cfg.dim_table, fact_count, inner_count, left_count;
        ELSE
            RAISE NOTICE '  OK % x % | joins stable: %.', cfg.fact_table, cfg.dim_table, fact_count;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Join Stability Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold join stability validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every Gold serving join is stable.';
    END IF;
END $$;
