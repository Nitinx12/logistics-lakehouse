-- Checks every databricks loader finished SUCCESS with no run left STARTED.
DO $$
DECLARE
    proc TEXT;
    success_count BIGINT;
    started_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver ETL Health';
    RAISE NOTICE '========================================';

    FOREACH proc IN ARRAY ARRAY[
        'silver.load_customers',
        'silver.load_drivers',
        'silver.load_facilities',
        'silver.load_routes',
        'silver.load_trailers',
        'silver.load_trucks',
        'silver.load_loads',
        'silver.load_trips',
        'silver.load_fuel_purchases',
        'silver.load_databricks_all'
    ]
    LOOP
        SELECT COUNT(*)
        INTO success_count
        FROM silver.etl_logs
        WHERE procedure_name = proc
            AND status = 'SUCCESS';

        SELECT COUNT(*)
        INTO started_count
        FROM silver.etl_logs
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
        RAISE EXCEPTION 'Silver ETL health validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every Silver loader is healthy.';
    END IF;
END $$;
