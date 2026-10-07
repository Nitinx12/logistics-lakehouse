-- Analytical marts for the dashboard; gold tables stay the system of record.
BEGIN;

CREATE OR REPLACE VIEW analytics.fleet_kpis AS
SELECT
    (SELECT COUNT(*) FROM gold.dim_truck WHERE truck_sk <> -1) AS truck_count,
    (SELECT COUNT(*) FROM gold.dim_trailer WHERE trailer_sk <> -1) AS trailer_count,
    (SELECT COUNT(*) FROM gold.dim_driver WHERE driver_sk <> -1) AS driver_count,
    (SELECT COALESCE(SUM(actual_distance_miles), 0) FROM gold.fact_trips) AS lifetime_miles,
    (SELECT COALESCE(SUM(fuel_gallons_used), 0) FROM gold.fact_trips) AS lifetime_gallons,
    (SELECT COALESCE(SUM(total_cost), 0) FROM gold.fact_fuel_purchases) AS lifetime_fuel_cost,
    (
        SELECT COALESCE(AVG(average_mpg), 0)
        FROM gold.fact_trips
        WHERE average_mpg > 0
    ) AS fleet_avg_mpg;

CREATE OR REPLACE VIEW analytics.fuel_monthly AS
SELECT
    dated.year_number AS year_number,
    dated.month_number AS month_number,
    dated.month_name AS month_name,
    COUNT(*) AS purchases,
    SUM(fuel.gallons) AS gallons,
    SUM(fuel.total_cost) AS total_cost,
    CASE
        WHEN SUM(fuel.gallons) > 0
        THEN SUM(fuel.total_cost) / SUM(fuel.gallons)
    END AS avg_price_per_gallon
FROM gold.fact_fuel_purchases AS fuel
INNER JOIN gold.dim_date AS dated ON dated.date_key = fuel.purchase_date_key
GROUP BY
    dated.year_number,
    dated.month_number,
    dated.month_name;

CREATE OR REPLACE VIEW analytics.truck_efficiency AS
SELECT
    truck.unit_number AS unit_number,
    truck.make AS make,
    COUNT(*) AS trips,
    SUM(trip.actual_distance_miles) AS miles,
    SUM(trip.fuel_gallons_used) AS gallons,
    SUM(trip.idle_time_hours) AS idle_hours,
    CASE
        WHEN SUM(trip.fuel_gallons_used) > 0
        THEN SUM(trip.actual_distance_miles) / SUM(trip.fuel_gallons_used)
    END AS mpg
FROM gold.fact_trips AS trip
INNER JOIN gold.dim_truck AS truck ON truck.truck_sk = trip.truck_sk
WHERE trip.truck_sk <> -1
GROUP BY
    truck.unit_number,
    truck.make;

CREATE OR REPLACE VIEW analytics.load_kpis AS
SELECT
    COUNT(*) AS loads,
    COALESCE(SUM(load.revenue), 0) AS revenue,
    COALESCE(SUM(load.weight_lbs), 0) AS weight_lbs,
    COALESCE(AVG(load.revenue), 0) AS avg_revenue_per_load,
    (
        SELECT AVG(CASE WHEN ev.on_time_flag THEN 1.0 ELSE 0.0 END)
        FROM gold.fact_delivery_events AS ev
        WHERE ev.event_type = 'DELIVERY'
    ) AS delivery_on_time_rate
FROM gold.fact_loads AS load;

CREATE OR REPLACE VIEW analytics.revenue_monthly AS
SELECT
    dated.year_number AS year_number,
    dated.month_number AS month_number,
    dated.month_name AS month_name,
    COUNT(*) AS loads,
    SUM(load.revenue) AS revenue,
    AVG(CASE WHEN ev.on_time_flag THEN 1.0 ELSE 0.0 END) AS delivery_on_time_rate
FROM gold.fact_loads AS load
INNER JOIN gold.dim_date AS dated ON dated.date_key = load.load_date_key
INNER JOIN gold.fact_delivery_events AS ev
    ON ev.load_id = load.load_id
    AND ev.event_type = 'DELIVERY'
GROUP BY
    dated.year_number,
    dated.month_number,
    dated.month_name;

CREATE OR REPLACE VIEW analytics.revenue_by_customer AS
SELECT
    cust.customer_name AS customer_name,
    cust.customer_type AS customer_type,
    COUNT(*) AS loads,
    SUM(load.revenue) AS revenue,
    SUM(load.revenue) / SUM(SUM(load.revenue)) OVER () AS revenue_share
FROM gold.fact_loads AS load
INNER JOIN gold.dim_customer AS cust ON cust.customer_sk = load.customer_sk
GROUP BY
    cust.customer_name,
    cust.customer_type;

CREATE OR REPLACE VIEW analytics.facility_delays AS
SELECT
    fac.facility_name AS facility_name,
    fac.city AS city,
    fac.state AS state,
    COUNT(*) AS delivery_events,
    AVG(ev.delay_minutes) AS avg_delay_minutes,
    AVG(ev.detention_minutes) AS avg_detention_minutes,
    AVG(CASE WHEN ev.on_time_flag THEN 1.0 ELSE 0.0 END) AS delivery_on_time_rate
FROM gold.fact_delivery_events AS ev
INNER JOIN gold.dim_facility AS fac ON fac.facility_sk = ev.facility_sk
WHERE ev.event_type = 'DELIVERY'
GROUP BY
    fac.facility_name,
    fac.city,
    fac.state;

CREATE OR REPLACE VIEW analytics.route_performance AS
WITH trip_delay AS (
    SELECT
        ev.trip_id AS trip_id,
        AVG(ev.delay_minutes) AS avg_delay_minutes
    FROM gold.fact_delivery_events AS ev
    WHERE ev.event_type = 'DELIVERY'
    GROUP BY ev.trip_id
)
SELECT
    route.origin_city || ' to ' || route.destination_city AS route_name,
    COUNT(*) AS trips,
    AVG(trip.actual_duration_hours) AS avg_actual_hours,
    AVG(route.typical_transit_days * 24.0) AS typical_hours,
    AVG(delay.avg_delay_minutes) AS avg_delay_minutes
FROM gold.fact_trips AS trip
INNER JOIN gold.fact_loads AS load ON load.load_id = trip.load_id
INNER JOIN gold.dim_route AS route ON route.route_sk = load.route_sk
LEFT JOIN trip_delay AS delay ON delay.trip_id = trip.trip_id
WHERE load.route_sk <> -1
GROUP BY
    route.origin_city,
    route.destination_city;

CREATE OR REPLACE VIEW analytics.safety_kpis AS
SELECT
    (SELECT COUNT(*) FROM gold.fact_safety_incidents) AS incidents,
    (SELECT COALESCE(SUM(claim_amount), 0) FROM gold.fact_safety_incidents) AS total_claims,
    (
        SELECT AVG(CASE WHEN preventable_flag THEN 1.0 ELSE 0.0 END)
        FROM gold.fact_safety_incidents
    ) AS preventable_rate,
    (
        SELECT AVG(CASE WHEN at_fault_flag THEN 1.0 ELSE 0.0 END)
        FROM gold.fact_safety_incidents
    ) AS at_fault_rate,
    (SELECT COUNT(*) FROM gold.fact_safety_incidents WHERE injury_flag) AS injuries,
    (
        SELECT COALESCE(SUM(parts_cost + labor_cost), 0)
        FROM gold.fact_maintenance
    ) AS maintenance_cost,
    (SELECT COALESCE(SUM(downtime_hours), 0) FROM gold.fact_maintenance) AS downtime_hours;

CREATE OR REPLACE VIEW analytics.claims_by_type AS
SELECT
    incident_type,
    COUNT(*) AS incidents,
    COALESCE(SUM(claim_amount), 0) AS claims,
    SUM(CASE WHEN preventable_flag THEN 1 ELSE 0 END) AS preventable,
    SUM(CASE WHEN injury_flag THEN 1 ELSE 0 END) AS injuries
FROM gold.fact_safety_incidents
GROUP BY incident_type;

CREATE OR REPLACE VIEW analytics.maintenance_by_truck AS
SELECT
    truck.unit_number AS unit_number,
    COUNT(*) AS records,
    SUM(maint.parts_cost + maint.labor_cost) AS cost,
    SUM(maint.downtime_hours) AS downtime_hours
FROM gold.fact_maintenance AS maint
INNER JOIN gold.dim_truck AS truck ON truck.truck_sk = maint.truck_sk
WHERE maint.truck_sk <> -1
GROUP BY truck.unit_number;

CREATE OR REPLACE VIEW analytics.incidents_monthly AS
SELECT
    dated.year_number AS year_number,
    dated.month_number AS month_number,
    dated.month_name AS month_name,
    COUNT(*) AS incidents,
    COALESCE(SUM(incident.claim_amount), 0) AS claims
FROM gold.fact_safety_incidents AS incident
INNER JOIN gold.dim_date AS dated ON dated.date_key = incident.incident_date_key
GROUP BY
    dated.year_number,
    dated.month_number,
    dated.month_name;

GRANT SELECT ON analytics.fleet_kpis TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.fuel_monthly TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.truck_efficiency TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.load_kpis TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.revenue_monthly TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.revenue_by_customer TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.facility_delays TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.route_performance TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.safety_kpis TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.claims_by_type TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.maintenance_by_truck TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.incidents_monthly TO analyst_ro, dashboard_ro, dq_runner;

COMMIT;
