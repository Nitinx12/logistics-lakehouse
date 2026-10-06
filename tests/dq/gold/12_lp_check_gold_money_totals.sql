-- Checks fact money totals reconcile to silver within one unit.
DO $$
DECLARE
    cfg RECORD;
    silver_total NUMERIC;
    gold_total NUMERIC;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Money Totals';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('loads', 'revenue', 'fact_loads', 'revenue'),
            ('fuel_purchases', 'total_cost', 'fact_fuel_purchases', 'total_cost'),
            ('maintenance_records', 'total_cost', 'fact_maintenance', 'total_cost')
        ) AS v(silver_table, silver_col, gold_table, gold_col)
    LOOP
        EXECUTE format(
            'SELECT COALESCE(SUM(%I::NUMERIC), 0) FROM silver.%I',
            cfg.silver_col,
            cfg.silver_table
        )
        INTO silver_total;

        EXECUTE format(
            'SELECT COALESCE(SUM(%I::NUMERIC), 0) FROM gold.%I',
            cfg.gold_col,
            cfg.gold_table
        )
        INTO gold_total;

        IF ABS(silver_total - gold_total) > 1.0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s (silver %s, gold %s); ',
                cfg.gold_table,
                silver_total,
                gold_total
            );
            RAISE NOTICE '  X % | silver: % | gold: %.', cfg.gold_table, silver_total, gold_total;
        ELSE
            RAISE NOTICE '  OK % | totals reconcile: %.', cfg.gold_table, gold_total;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Money Total Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Gold money total validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'All Gold money totals reconcile.';
    END IF;
END $$;
