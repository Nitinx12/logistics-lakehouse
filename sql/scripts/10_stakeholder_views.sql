-- Stakeholder marts: margin, driver scorecards, mix, delay buckets, cohorts.
BEGIN;

CREATE OR REPLACE VIEW analytics.monthly_pnl AS
WITH revenue AS (
    SELECT
        dated.year_number AS year_number,
        dated.month_number AS month_number,
        dated.month_name AS month_name,
        SUM(load.revenue) AS revenue
    FROM gold.fact_loads AS load
    INNER JOIN gold.dim_date AS dated ON dated.date_key = load.load_date_key
    GROUP BY
        dated.year_number,
        dated.month_number,
        dated.month_name
),
fuel AS (
    SELECT
        dated.year_number AS year_number,
        dated.month_number AS month_number,
        SUM(fuel.total_cost) AS fuel_cost
    FROM gold.fact_fuel_purchases AS fuel
    INNER JOIN gold.dim_date AS dated ON dated.date_key = fuel.purchase_date_key
    GROUP BY
        dated.year_number,
        dated.month_number
),
upkeep AS (
    SELECT
        dated.year_number AS year_number,
        dated.month_number AS month_number,
        SUM(maint.parts_cost + maint.labor_cost) AS maintenance_cost
    FROM gold.fact_maintenance AS maint
    INNER JOIN gold.dim_date AS dated ON dated.date_key = maint.maintenance_date_key
    GROUP BY
        dated.year_number,
        dated.month_number
),
claims AS (
    SELECT
        dated.year_number AS year_number,
        dated.month_number AS month_number,
        SUM(incident.claim_amount) AS claim_cost
    FROM gold.fact_safety_incidents AS incident
    INNER JOIN gold.dim_date AS dated ON dated.date_key = incident.incident_date_key
    GROUP BY
        dated.year_number,
        dated.month_number
),
detention AS (
    SELECT
        dated.year_number AS year_number,
        dated.month_number AS month_number,
        SUM(ev.detention_minutes) / 60.0 AS detention_hours
    FROM gold.fact_delivery_events AS ev
    INNER JOIN gold.fact_loads AS load ON load.load_id = ev.load_id
    INNER JOIN gold.dim_date AS dated ON dated.date_key = load.load_date_key
    WHERE ev.event_type = 'DELIVERY'
    GROUP BY
        dated.year_number,
        dated.month_number
),
months AS (
    SELECT year_number, month_number FROM revenue
    UNION
    SELECT year_number, month_number FROM fuel
    UNION
    SELECT year_number, month_number FROM upkeep
    UNION
    SELECT year_number, month_number FROM claims
),
month_names AS (
    SELECT DISTINCT year_number, month_number, month_name
    FROM gold.dim_date
)
SELECT
    months.year_number AS year_number,
    months.month_number AS month_number,
    month_names.month_name AS month_name,
    COALESCE(revenue.revenue, 0) AS revenue,
    COALESCE(fuel.fuel_cost, 0) AS fuel_cost,
    COALESCE(upkeep.maintenance_cost, 0) AS maintenance_cost,
    COALESCE(claims.claim_cost, 0) AS claim_cost,
    COALESCE(detention.detention_hours, 0) AS detention_hours,
    COALESCE(revenue.revenue, 0)
        - COALESCE(fuel.fuel_cost, 0)
        - COALESCE(upkeep.maintenance_cost, 0)
        - COALESCE(claims.claim_cost, 0) AS gross_margin,
    CASE
        WHEN COALESCE(revenue.revenue, 0) > 0
        THEN (
            COALESCE(revenue.revenue, 0)
            - COALESCE(fuel.fuel_cost, 0)
            - COALESCE(upkeep.maintenance_cost, 0)
            - COALESCE(claims.claim_cost, 0)
        ) / revenue.revenue
    END AS gross_margin_rate
FROM months
LEFT JOIN month_names
    ON month_names.year_number = months.year_number
    AND month_names.month_number = months.month_number
LEFT JOIN revenue
    ON revenue.year_number = months.year_number
    AND revenue.month_number = months.month_number
LEFT JOIN fuel
    ON fuel.year_number = months.year_number
    AND fuel.month_number = months.month_number
LEFT JOIN upkeep
    ON upkeep.year_number = months.year_number
    AND upkeep.month_number = months.month_number
LEFT JOIN claims
    ON claims.year_number = months.year_number
    AND claims.month_number = months.month_number
LEFT JOIN detention
    ON detention.year_number = months.year_number
    AND detention.month_number = months.month_number;

CREATE OR REPLACE VIEW analytics.driver_scorecard AS
WITH trip_stats AS (
    SELECT
        trip.driver_sk AS driver_sk,
        COUNT(*) AS trips,
        SUM(trip.actual_distance_miles) AS miles,
        SUM(trip.fuel_gallons_used) AS gallons,
        SUM(trip.idle_time_hours) AS idle_hours
    FROM gold.fact_trips AS trip
    WHERE trip.driver_sk <> -1
    GROUP BY trip.driver_sk
),
incident_stats AS (
    SELECT
        incident.driver_sk AS driver_sk,
        COUNT(*) AS incidents,
        SUM(CASE WHEN incident.at_fault_flag THEN 1 ELSE 0 END) AS at_fault,
        COALESCE(SUM(incident.claim_amount), 0) AS claims
    FROM gold.fact_safety_incidents AS incident
    WHERE incident.driver_sk <> -1
    GROUP BY incident.driver_sk
)
SELECT
    drv.first_name || ' ' || drv.last_name AS driver_name,
    drv.employment_status AS employment_status,
    drv.home_terminal AS home_terminal,
    COALESCE(trip_stats.trips, 0) AS trips,
    COALESCE(trip_stats.miles, 0) AS miles,
    CASE
        WHEN COALESCE(trip_stats.gallons, 0) > 0
        THEN trip_stats.miles / trip_stats.gallons
    END AS mpg,
    COALESCE(trip_stats.idle_hours, 0) AS idle_hours,
    COALESCE(incident_stats.incidents, 0) AS incidents,
    COALESCE(incident_stats.at_fault, 0) AS at_fault,
    COALESCE(incident_stats.claims, 0) AS claims
FROM gold.dim_driver AS drv
LEFT JOIN trip_stats ON trip_stats.driver_sk = drv.driver_sk
LEFT JOIN incident_stats ON incident_stats.driver_sk = drv.driver_sk
WHERE drv.driver_sk <> -1;

CREATE OR REPLACE VIEW analytics.load_mix AS
SELECT
    load_type,
    load_status,
    booking_type,
    COUNT(*) AS loads,
    COALESCE(SUM(revenue), 0) AS revenue,
    COALESCE(SUM(weight_lbs), 0) AS weight_lbs
FROM gold.fact_loads
GROUP BY
    load_type,
    load_status,
    booking_type;

CREATE OR REPLACE VIEW analytics.delay_buckets AS
SELECT
    CASE
        WHEN delay_minutes <= 0 THEN 'Early or on time'
        WHEN delay_minutes <= 30 THEN 'Up to 30 min late'
        WHEN delay_minutes <= 60 THEN '30 to 60 min late'
        WHEN delay_minutes <= 120 THEN '1 to 2 hours late'
        ELSE 'Over 2 hours late'
    END AS delay_bucket,
    COUNT(*) AS delivery_events,
    COUNT(*) / SUM(COUNT(*)) OVER () AS event_share,
    AVG(detention_minutes) AS avg_detention_minutes
FROM gold.fact_delivery_events
WHERE event_type = 'DELIVERY'
GROUP BY 1;

CREATE OR REPLACE VIEW analytics.customer_monthly AS
SELECT
    cust.customer_name AS customer_name,
    cust.customer_type AS customer_type,
    dated.year_number AS year_number,
    dated.month_number AS month_number,
    dated.month_name AS month_name,
    COUNT(*) AS loads,
    SUM(load.revenue) AS revenue
FROM gold.fact_loads AS load
INNER JOIN gold.dim_customer AS cust ON cust.customer_sk = load.customer_sk
INNER JOIN gold.dim_date AS dated ON dated.date_key = load.load_date_key
GROUP BY
    cust.customer_name,
    cust.customer_type,
    dated.year_number,
    dated.month_number,
    dated.month_name;

CREATE OR REPLACE VIEW analytics.facility_monthly AS
SELECT
    fac.facility_name AS facility_name,
    fac.city AS city,
    fac.state AS state,
    dated.year_number AS year_number,
    dated.month_number AS month_number,
    COUNT(*) AS delivery_events,
    AVG(ev.delay_minutes) AS avg_delay_minutes,
    AVG(CASE WHEN ev.on_time_flag THEN 1.0 ELSE 0.0 END) AS delivery_on_time_rate
FROM gold.fact_delivery_events AS ev
INNER JOIN gold.dim_facility AS fac ON fac.facility_sk = ev.facility_sk
INNER JOIN gold.fact_loads AS load ON load.load_id = ev.load_id
INNER JOIN gold.dim_date AS dated ON dated.date_key = load.load_date_key
WHERE ev.event_type = 'DELIVERY'
GROUP BY
    fac.facility_name,
    fac.city,
    fac.state,
    dated.year_number,
    dated.month_number;

GRANT SELECT ON analytics.monthly_pnl TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.driver_scorecard TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.load_mix TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.delay_buckets TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.customer_monthly TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.facility_monthly TO analyst_ro, dashboard_ro, dq_runner;

COMMIT;
