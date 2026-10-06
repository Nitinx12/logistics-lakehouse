-- Checks facility coordinates fall on planet Earth.
DO $$
DECLARE
    bad_count BIGINT;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Facility Coordinates';
    RAISE NOTICE '========================================';

    SELECT COUNT(*)
    INTO bad_count
    FROM silver.facilities
    WHERE latitude NOT BETWEEN -90 AND 90
        OR longitude NOT BETWEEN -180 AND 180;

    IF bad_count > 0 THEN
        RAISE EXCEPTION 'Silver coordinate validation FAILED: % rows out of range.', bad_count;
    ELSE
        RAISE NOTICE '  OK facilities | coordinates in range.';
    END IF;

    RAISE NOTICE 'Coordinate Check Complete';
END $$;
