-- Checks money totals reconcile between silver and bronze within one unit.
DO $$
DECLARE
    cfg RECORD;
    bronze_total NUMERIC;
    silver_total NUMERIC;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Money Totals';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('loads', 'revenue'),
            ('fuel_purchases', 'total_cost'),
            ('maintenance_records', 'total_cost')
        ) AS v(table_name, column_name)
    LOOP
        EXECUTE format(
            'SELECT COALESCE(SUM(%I::NUMERIC), 0) FROM bronze.%I',
            cfg.column_name,
            cfg.table_name
        )
        INTO bronze_total;

        EXECUTE format(
            'SELECT COALESCE(SUM(%I::NUMERIC), 0) FROM silver.%I',
            cfg.column_name,
            cfg.table_name
        )
        INTO silver_total;

        IF ABS(bronze_total - silver_total) > 1.0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s.%s (bronze %s, silver %s); ',
                cfg.table_name,
                cfg.column_name,
                bronze_total,
                silver_total
            );
            RAISE NOTICE '  X %.% | bronze: % | silver: %.', cfg.table_name, cfg.column_name, bronze_total, silver_total;
        ELSE
            RAISE NOTICE '  OK %.% | totals reconcile: %.', cfg.table_name, cfg.column_name, silver_total;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Money Total Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Silver money total validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Silver money totals reconcile.';
    END IF;
END $$;
