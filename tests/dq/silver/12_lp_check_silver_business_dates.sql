-- Checks no business date lands in the future.
DO $$
DECLARE
    cfg RECORD;
    future_count BIGINT;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Business Dates';
    RAISE NOTICE '========================================';

    FOR cfg IN
        SELECT * FROM (VALUES
            ('loads', 'load_date'),
            ('trips', 'dispatch_date'),
            ('fuel_purchases', 'purchase_date'),
            ('trucks', 'acquisition_date'),
            ('trailers', 'acquisition_date'),
            ('customers', 'contract_start_date'),
            ('drivers', 'hire_date'),
            ('maintenance_records', 'maintenance_date'),
            ('safety_incidents', 'incident_date'),
            ('delivery_events', 'scheduled_datetime'),
            ('delivery_events', 'actual_datetime')
        ) AS v(table_name, column_name)
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM silver.%I WHERE %I::DATE > CURRENT_DATE',
            cfg.table_name,
            cfg.column_name
        )
        INTO future_count;

        IF future_count > 0 THEN
            any_failed := TRUE;
            fail_msg := fail_msg || format(
                '%s.%s (%s future); ',
                cfg.table_name,
                cfg.column_name,
                future_count
            );
            RAISE NOTICE '  X %.% | future: %.', cfg.table_name, cfg.column_name, future_count;
        ELSE
            RAISE NOTICE '  OK %.% | no future dates.', cfg.table_name, cfg.column_name;
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Business Date Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Silver business date validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'No Silver business date is in the future.';
    END IF;
END $$;
