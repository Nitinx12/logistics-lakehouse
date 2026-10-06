-- Checks fact mpg reconciles to distance over fuel within tolerance.
DO $$
DECLARE
    bad_count BIGINT;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Gold Trip MPG Math';
    RAISE NOTICE '========================================';

    SELECT COUNT(*)
    INTO bad_count
    FROM gold.fact_trips
    WHERE ABS(average_mpg - actual_distance_miles / fuel_gallons_used)
        > GREATEST(0.5, average_mpg * 0.1);

    IF bad_count > 0 THEN
        RAISE EXCEPTION 'Gold trip mpg validation FAILED: % rows mismatch.', bad_count;
    ELSE
        RAISE NOTICE '  OK fact_trips | mpg reconciles.';
    END IF;

    RAISE NOTICE 'Trip MPG Check Complete';
END $$;
