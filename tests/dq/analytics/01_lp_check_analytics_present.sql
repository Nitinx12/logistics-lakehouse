-- Checks every dashboard mart exists with rows: single row KPIs, many row marts.
DO $$
DECLARE
    cfg RECORD;
    view_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Analytics Presence';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('fleet_kpis', 1),
            ('fuel_monthly', 0),
            ('truck_efficiency', 0),
            ('load_kpis', 1),
            ('revenue_monthly', 0),
            ('revenue_by_customer', 0),
            ('facility_delays', 0),
            ('route_performance', 0),
            ('safety_kpis', 1),
            ('claims_by_type', 0),
            ('maintenance_by_truck', 0),
            ('incidents_monthly', 0),
            ('monthly_pnl', 0),
            ('driver_scorecard', 0),
            ('load_mix', 0),
            ('delay_buckets', 0),
            ('customer_monthly', 0),
            ('facility_monthly', 0),
            ('maintenance_monthly', 0),
            ('maintenance_by_type', 0),
            ('truck_scorecard', 0),
            ('incidents_by_location', 0),
            ('safety_monthly', 0),
            ('facility_safety', 0),
            ('driver_monthly', 0),
            ('truck_monthly', 0)
        ) AS v(view_name, exact_one)
    LOOP
        BEGIN
            EXECUTE format('SELECT COUNT(*) FROM analytics.%I', cfg.view_name)
            INTO view_count;
        EXCEPTION
            WHEN undefined_table THEN
                any_failed := TRUE;
                fail_msg := fail_msg || format('%s (missing); ', cfg.view_name);
                RAISE NOTICE '  X View: % | missing.', cfg.view_name;
                CONTINUE;
        END;

        IF view_count < 1 OR (cfg.exact_one = 1 AND view_count <> 1) THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format('%s (rows %s); ', cfg.view_name, view_count);
            RAISE NOTICE '  X View: % | rows: %.', cfg.view_name, view_count;
        ELSE
            RAISE NOTICE '  OK View: % | rows: %.', cfg.view_name, view_count;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Presence Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Analytics presence validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every dashboard mart is present with rows.';
    END IF;
END $$;
