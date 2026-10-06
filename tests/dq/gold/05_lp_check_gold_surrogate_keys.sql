-- Checks surrogate keys are unique with unknown members present.
DO $$
DECLARE
    cfg RECORD;
    null_count BIGINT;
    dup_count BIGINT;
    unknown_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Surrogate Keys';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('dim_date', 'date_key'),
            ('dim_customer', 'customer_sk'),
            ('dim_driver', 'driver_sk'),
            ('dim_facility', 'facility_sk'),
            ('dim_route', 'route_sk'),
            ('dim_trailer', 'trailer_sk'),
            ('dim_truck', 'truck_sk')
        ) AS v(table_name, column_name)
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I WHERE %I IS NULL',
            cfg.table_name,
            cfg.column_name
        )
        INTO null_count;

        EXECUTE format(
            'SELECT COUNT(*) FROM (SELECT %I FROM gold.%I GROUP BY %I HAVING COUNT(*) > 1) AS dupes',
            cfg.column_name,
            cfg.table_name,
            cfg.column_name
        )
        INTO dup_count;

        EXECUTE format(
            'SELECT COUNT(*) FROM gold.%I WHERE %I = -1',
            cfg.table_name,
            cfg.column_name
        )
        INTO unknown_count;

        IF null_count > 0 OR dup_count > 0 OR unknown_count <> 1 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s (null %s, dupe %s, unknown %s); ',
                cfg.table_name,
                null_count,
                dup_count,
                unknown_count
            );
            RAISE NOTICE '  X Table: % | null: % | dupe: % | unknown: %.', cfg.table_name, null_count, dup_count, unknown_count;
        ELSE
            RAISE NOTICE '  OK Table: % | surrogates unique, unknown present.', cfg.table_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Surrogate Key Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold surrogate key validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Gold surrogate keys are sound.';
    END IF;
END $$;
