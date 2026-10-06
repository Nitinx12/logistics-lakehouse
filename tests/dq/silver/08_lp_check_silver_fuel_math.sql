-- Checks fuel totals reconcile to gallons times price within tolerance.
DO $$
DECLARE
    bad_count BIGINT;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Fuel Math';
    RAISE NOTICE '========================================';

    SELECT COUNT(*)
    INTO bad_count
    FROM silver.fuel_purchases
    WHERE ABS(total_cost - gallons * price_per_gallon)
        > GREATEST(1.0, total_cost * 0.02);

    IF bad_count > 0 THEN
        RAISE EXCEPTION 'Silver fuel math validation FAILED: % rows mismatch.', bad_count;
    ELSE
        RAISE NOTICE '  OK fuel_purchases | totals reconcile.';
    END IF;

    RAISE NOTICE 'Fuel Math Check Complete';
END $$;
