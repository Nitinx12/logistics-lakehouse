-- Checks money and measure columns never hold negative or zero where forbidden.
DO $$
DECLARE
    tbl RECORD;
    predicate TEXT;
    bad_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Non Negative Measures';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'silver'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs')
        ORDER BY table_name
    LOOP
        predicate := CASE tbl.table_name
            WHEN 'customers' THEN 'credit_terms_days < 0 OR annual_revenue_potential < 0'
            WHEN 'drivers' THEN 'years_experience < 0'
            WHEN 'facilities' THEN 'dock_doors < 0'
            WHEN 'routes' THEN 'typical_distance_miles <= 0 OR base_rate_per_mile <= 0 OR fuel_surcharge_rate < 0 OR typical_transit_days <= 0'
            WHEN 'trailers' THEN 'trailer_number <= 0 OR length_feet <= 0 OR model_year <= 0'
            WHEN 'trucks' THEN 'unit_number <= 0 OR model_year <= 0 OR acquisition_mileage < 0 OR tank_capacity_gallons <= 0'
            WHEN 'loads' THEN 'weight_lbs <= 0 OR pieces <= 0 OR revenue < 0 OR fuel_surcharge < 0 OR accessorial_charges < 0'
            WHEN 'trips' THEN 'actual_distance_miles <= 0 OR actual_duration_hours <= 0 OR fuel_gallons_used <= 0 OR average_mpg <= 0 OR idle_time_hours < 0'
            WHEN 'fuel_purchases' THEN 'gallons <= 0 OR price_per_gallon <= 0 OR total_cost < 0'
            WHEN 'delivery_events' THEN 'detention_minutes < 0'
            WHEN 'maintenance_records' THEN 'odometer_reading < 0 OR labor_hours < 0 OR labor_cost < 0 OR parts_cost < 0 OR total_cost < 0 OR downtime_hours < 0'
            WHEN 'safety_incidents' THEN 'vehicle_damage_cost < 0 OR cargo_damage_cost < 0 OR claim_amount < 0'
            ELSE NULL
        END;

        IF predicate IS NULL THEN
            RAISE NOTICE '  - Table: % | no measure rules, skipped.', tbl.table_name;
            CONTINUE;
        END IF;

        EXECUTE format('SELECT COUNT(*) FROM silver.%I WHERE %s', tbl.table_name, predicate)
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
        RAISE EXCEPTION 'Silver measure validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Silver measures are in range.';
    END IF;
END $$;
