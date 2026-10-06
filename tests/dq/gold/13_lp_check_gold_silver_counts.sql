-- Checks gold row counts reconcile to silver: facts match, dims add unknown.
DO $$
DECLARE
    cfg RECORD;
    silver_count BIGINT;
    gold_count BIGINT;
    expected BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Silver Counts';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('customers', 'dim_customer', 1),
            ('drivers', 'dim_driver', 1),
            ('facilities', 'dim_facility', 1),
            ('routes', 'dim_route', 1),
            ('trailers', 'dim_trailer', 1),
            ('trucks', 'dim_truck', 1),
            ('loads', 'fact_loads', 0),
            ('trips', 'fact_trips', 0),
            ('fuel_purchases', 'fact_fuel_purchases', 0),
            ('delivery_events', 'fact_delivery_events', 0),
            ('maintenance_records', 'fact_maintenance', 0),
            ('safety_incidents', 'fact_safety_incidents', 0)
        ) AS v(silver_table, gold_table, unknown_extra)
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM silver.%I', cfg.silver_table)
        INTO silver_count;

        EXECUTE format('SELECT COUNT(*) FROM gold.%I', cfg.gold_table)
        INTO gold_count;

        expected := silver_count + cfg.unknown_extra;

        IF gold_count <> expected THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s (gold %s, expected %s); ',
                cfg.gold_table,
                gold_count,
                expected
            );
            RAISE NOTICE '  X Table: % | gold: % | expected: %.', cfg.gold_table, gold_count, expected;
        ELSE
            RAISE NOTICE '  OK Table: % | counts match: %.', cfg.gold_table, gold_count;
        END IF;
    END LOOP;

    SELECT (DATE '2030-12-31' - DATE '2010-01-01' + 1) + 1
    INTO expected;

    SELECT COUNT(*)
    INTO gold_count
    FROM gold.dim_date;

    IF gold_count <> expected THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format(
            'dim_date (gold %s, expected %s); ',
            gold_count,
            expected
        );
        RAISE NOTICE '  X Table: dim_date | gold: % | expected: %.', gold_count, expected;
    ELSE
        RAISE NOTICE '  OK Table: dim_date | counts match: %.', gold_count;
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Count Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold count validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Gold reconciles to Silver on every table.';
    END IF;
END $$;
