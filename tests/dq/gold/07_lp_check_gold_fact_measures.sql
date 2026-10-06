-- Checks fact money and measure columns never hold forbidden values.
DO $$
DECLARE
    tbl RECORD;
    predicate TEXT;
    bad_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Fact Measures';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'gold'
            AND table_type = 'BASE TABLE'
            AND table_name LIKE 'fact\_%'
        ORDER BY table_name
    LOOP
        predicate := CASE tbl.table_name
            WHEN 'fact_loads' THEN 'weight_lbs <= 0 OR pieces <= 0 OR revenue < 0 OR fuel_surcharge < 0 OR accessorial_charges < 0'
            WHEN 'fact_trips' THEN 'actual_distance_miles <= 0 OR actual_duration_hours <= 0 OR fuel_gallons_used <= 0 OR average_mpg <= 0 OR idle_time_hours < 0'
            WHEN 'fact_fuel_purchases' THEN 'gallons <= 0 OR price_per_gallon <= 0 OR total_cost < 0'
            WHEN 'fact_delivery_events' THEN 'detention_minutes < 0'
            WHEN 'fact_maintenance' THEN 'odometer_reading < 0 OR labor_hours < 0 OR labor_cost < 0 OR parts_cost < 0 OR total_cost < 0 OR downtime_hours < 0'
            WHEN 'fact_safety_incidents' THEN 'vehicle_damage_cost < 0 OR cargo_damage_cost < 0 OR claim_amount < 0'
            ELSE NULL
        END;

        IF predicate IS NULL THEN
            RAISE NOTICE '  - Table: % | no measure rules, skipped.', tbl.table_name;
            CONTINUE;
        END IF;

        EXECUTE format('SELECT COUNT(*) FROM gold.%I WHERE %s', tbl.table_name, predicate)
        INTO bad_count;

        IF bad_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format('%s (%s rows); ', tbl.table_name, bad_count);
            RAISE NOTICE '  X Table: % | bad measures: %.', tbl.table_name, bad_count;
        ELSE
            RAISE NOTICE '  OK Table: % | measures valid.', tbl.table_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Measure Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold measure validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Gold measures are in range.';
    END IF;
END $$;
