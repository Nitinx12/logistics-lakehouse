-- Checks every gold loader finished SUCCESS with no run left STARTED.
DO $$
DECLARE
    proc TEXT;
    success_count BIGINT;
    started_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold ETL Health';
    RAISE NOTICE '========================================';

    FOREACH proc IN ARRAY ARRAY[
        'gold.load_dim_date',
        'gold.load_dim_customers',
        'gold.load_dim_drivers',
        'gold.load_dim_facilities',
        'gold.load_dim_routes',
        'gold.load_dim_trailers',
        'gold.load_dim_trucks',
        'gold.load_fact_loads',
        'gold.load_fact_trips',
        'gold.load_fact_fuel_purchases',
        'gold.load_fact_delivery_events',
        'gold.load_fact_maintenance',
        'gold.load_fact_safety_incidents',
        'gold.load_gold_all'
    ]
    LOOP
        SELECT COUNT(*)
        INTO success_count
        FROM gold.etl_logs
        WHERE procedure_name = proc
            AND status = 'SUCCESS';

        SELECT COUNT(*)
        INTO started_count
        FROM gold.etl_logs
        WHERE procedure_name = proc
            AND status = 'STARTED';

        IF success_count = 0 OR started_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s (%s success, %s started); ',
                proc,
                success_count,
                started_count
            );
            RAISE NOTICE '  X % | success: % | started: %.', proc, success_count, started_count;
        ELSE
            RAISE NOTICE '  OK % | success: %.', proc, success_count;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'ETL Health Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold ETL health validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every Gold loader is healthy.';
    END IF;
END $$;
