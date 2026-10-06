-- Checks silver business keys have no nulls and no duplicates.
DO $$
DECLARE
    tbl RECORD;
    key_col TEXT;
    null_count BIGINT;
    dup_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Primary Keys';
    RAISE NOTICE '========================================';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'silver'
            AND table_type = 'BASE TABLE'
            AND table_name NOT IN ('etl_logs')
        ORDER BY table_name
    LOOP
        key_col := CASE tbl.table_name
            WHEN 'customers' THEN 'customer_id'
            WHEN 'drivers' THEN 'driver_id'
            WHEN 'facilities' THEN 'facility_id'
            WHEN 'routes' THEN 'route_id'
            WHEN 'trailers' THEN 'trailer_id'
            WHEN 'trucks' THEN 'truck_id'
            WHEN 'loads' THEN 'load_id'
            WHEN 'trips' THEN 'trip_id'
            WHEN 'fuel_purchases' THEN 'fuel_purchase_id'
            WHEN 'delivery_events' THEN 'event_id'
            WHEN 'maintenance_records' THEN 'maintenance_id'
            WHEN 'safety_incidents' THEN 'incident_id'
            ELSE NULL
        END;

        IF key_col IS NULL THEN
            RAISE NOTICE '  - Table: % | no known key, skipped.', tbl.table_name;
            CONTINUE;
        END IF;

        EXECUTE format(
            'SELECT COUNT(*) FROM silver.%I WHERE %I IS NULL',
            tbl.table_name,
            key_col
        )
        INTO null_count;

        EXECUTE format(
            'SELECT COUNT(*) FROM (SELECT %I FROM silver.%I GROUP BY %I HAVING COUNT(*) > 1) AS dupes',
            key_col,
            tbl.table_name,
            key_col
        )
        INTO dup_count;

        IF null_count > 0 OR dup_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s (key %s: %s null, %s duplicate); ',
                tbl.table_name,
                key_col,
                null_count,
                dup_count
            );
            RAISE NOTICE '  X Table: % | key: % | null: % | duplicate: %.', tbl.table_name, key_col, null_count, dup_count;
        ELSE
            RAISE NOTICE '  OK Table: % | key: % | null: 0 | duplicate: 0.', tbl.table_name, key_col;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Primary Key Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Silver primary key validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Silver business keys are unique and not null.';
    END IF;
END $$;
