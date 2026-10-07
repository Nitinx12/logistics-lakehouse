-- Checks dashboard marts reconcile to gold: same totals, no drift.
DO $$
DECLARE
    mart_total NUMERIC;
    gold_total NUMERIC;
    any_failed BOOLEAN := FALSE;
    fail_msg TEXT := '';
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Checking Analytics Reconciliation';
    RAISE NOTICE '========================================';

    SELECT SUM(revenue) INTO mart_total FROM analytics.revenue_monthly;
    SELECT SUM(revenue) INTO gold_total FROM gold.fact_loads;
    IF ROUND(mart_total, 2) IS DISTINCT FROM ROUND(gold_total, 2) THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('revenue (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X revenue | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK revenue | total: %.', mart_total;
    END IF;

    SELECT SUM(total_cost) INTO mart_total FROM analytics.fuel_monthly;
    SELECT SUM(total_cost) INTO gold_total FROM gold.fact_fuel_purchases;
    IF ROUND(mart_total, 2) IS DISTINCT FROM ROUND(gold_total, 2) THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('fuel cost (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X fuel cost | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK fuel cost | total: %.', mart_total;
    END IF;

    SELECT SUM(claims) INTO mart_total FROM analytics.claims_by_type;
    SELECT SUM(claim_amount) INTO gold_total FROM gold.fact_safety_incidents;
    IF mart_total IS DISTINCT FROM gold_total THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('claims (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X claims | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK claims | total: %.', mart_total;
    END IF;

    SELECT SUM(incidents) INTO mart_total FROM analytics.incidents_monthly;
    SELECT COUNT(*) INTO gold_total FROM gold.fact_safety_incidents;
    IF mart_total IS DISTINCT FROM gold_total THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('incidents (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X incidents | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK incidents | total: %.', mart_total;
    END IF;

    SELECT SUM(revenue) INTO mart_total FROM analytics.monthly_pnl;
    SELECT SUM(revenue) INTO gold_total FROM gold.fact_loads;
    IF ROUND(mart_total, 2) IS DISTINCT FROM ROUND(gold_total, 2) THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('pnl revenue (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X pnl revenue | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK pnl revenue | total: %.', mart_total;
    END IF;

    SELECT SUM(claim_cost) INTO mart_total FROM analytics.monthly_pnl;
    SELECT SUM(claim_amount) INTO gold_total FROM gold.fact_safety_incidents;
    IF mart_total IS DISTINCT FROM gold_total THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('pnl claims (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X pnl claims | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK pnl claims | total: %.', mart_total;
    END IF;

    SELECT SUM(loads) INTO mart_total FROM analytics.load_mix;
    SELECT COUNT(*) INTO gold_total FROM gold.fact_loads;
    IF mart_total IS DISTINCT FROM gold_total THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('load mix (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X load mix | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK load mix | total: %.', mart_total;
    END IF;

    SELECT SUM(delivery_events) INTO mart_total FROM analytics.delay_buckets;
    SELECT COUNT(*) INTO gold_total FROM gold.fact_delivery_events WHERE event_type = 'DELIVERY';
    IF mart_total IS DISTINCT FROM gold_total THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('delay buckets (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X delay buckets | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK delay buckets | total: %.', mart_total;
    END IF;

    SELECT SUM(total_cost) INTO mart_total FROM analytics.maintenance_monthly;
    SELECT SUM(parts_cost + labor_cost) INTO gold_total FROM gold.fact_maintenance;
    IF ROUND(mart_total, 2) IS DISTINCT FROM ROUND(gold_total, 2) THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('upkeep monthly (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X upkeep monthly | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK upkeep monthly | total: %.', mart_total;
    END IF;

    SELECT SUM(incidents) INTO mart_total FROM analytics.safety_monthly;
    SELECT COUNT(*) INTO gold_total FROM gold.fact_safety_incidents;
    IF mart_total IS DISTINCT FROM gold_total THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('safety monthly (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X safety monthly | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK safety monthly | total: %.', mart_total;
    END IF;

    SELECT SUM(trips) INTO mart_total FROM analytics.truck_scorecard;
    SELECT COUNT(*) INTO gold_total FROM gold.fact_trips WHERE truck_sk <> -1;
    IF mart_total IS DISTINCT FROM gold_total THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('truck trips (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X truck trips | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK truck trips | total: %.', mart_total;
    END IF;

    SELECT SUM(total_miles) INTO mart_total FROM analytics.driver_monthly;
    SELECT SUM(actual_distance_miles) INTO gold_total FROM gold.fact_trips WHERE driver_sk <> -1;
    IF mart_total IS DISTINCT FROM gold_total THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('driver miles (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X driver miles | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK driver miles | total: %.', mart_total;
    END IF;

    SELECT SUM(trips_completed) INTO mart_total FROM analytics.truck_monthly;
    SELECT COUNT(*) INTO gold_total FROM gold.fact_trips WHERE truck_sk <> -1;
    IF mart_total IS DISTINCT FROM gold_total THEN
        any_failed := TRUE;
        fail_msg := fail_msg || format('truck months (mart %s, gold %s); ', mart_total, gold_total);
        RAISE NOTICE '  X truck months | mart: % | gold: %.', mart_total, gold_total;
    ELSE
        RAISE NOTICE '  OK truck months | total: %.', mart_total;
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Reconciliation Check Complete';
    RAISE NOTICE '========================================';

    IF any_failed THEN
        RAISE EXCEPTION 'Analytics reconciliation FAILED: %', fail_msg;
    ELSE
        RAISE NOTICE 'Every dashboard mart reconciles to gold.';
    END IF;
END $$;
