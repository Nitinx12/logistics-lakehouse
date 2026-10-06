-- Checks the one to one grain between loads and trips holds both ways.
DO $$
DECLARE
    loads_without_trip BIGINT;
    trips_without_load BIGINT;
    loads_with_many_trips BIGINT;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Silver Load Trip Grain';
    RAISE NOTICE '========================================';

    SELECT COUNT(*)
    INTO loads_without_trip
    FROM silver.loads AS l
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.trips AS t WHERE t.load_id = l.load_id
    );

    SELECT COUNT(*)
    INTO trips_without_load
    FROM silver.trips AS t
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.loads AS l WHERE l.load_id = t.load_id
    );

    SELECT COUNT(*)
    INTO loads_with_many_trips
    FROM (
        SELECT load_id
        FROM silver.trips
        GROUP BY load_id
        HAVING COUNT(*) > 1
    ) AS dupes;

    IF loads_without_trip > 0 THEN
        fail_msg := fail_msg || format('%s loads without trip; ', loads_without_trip);
    END IF;

    IF trips_without_load > 0 THEN
        fail_msg := fail_msg || format('%s trips without load; ', trips_without_load);
    END IF;

    IF loads_with_many_trips > 0 THEN
        fail_msg := fail_msg || format('%s loads with many trips; ', loads_with_many_trips);
    END IF;

    IF fail_msg <> '' THEN
        RAISE EXCEPTION 'Silver grain validation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE '  OK loads <-> trips | one to one grain holds.';
    END IF;

    RAISE NOTICE 'Grain Check Complete';
END $$;
