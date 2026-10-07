-- Safety and maintenance marts across trucks, drivers, types, places, months.
BEGIN;

CREATE OR REPLACE VIEW analytics.maintenance_monthly AS
SELECT
    dated.year_number AS year_number,
    dated.month_number AS month_number,
    dated.month_name AS month_name,
    COUNT(*) AS records,
    SUM(maint.parts_cost) AS parts_cost,
    SUM(maint.labor_cost) AS labor_cost,
    SUM(maint.parts_cost + maint.labor_cost) AS total_cost,
    SUM(maint.downtime_hours) AS downtime_hours,
    AVG(maint.odometer_reading) AS avg_odometer_reading
FROM gold.fact_maintenance AS maint
INNER JOIN gold.dim_date AS dated ON dated.date_key = maint.maintenance_date_key
GROUP BY
    dated.year_number,
    dated.month_number,
    dated.month_name;

CREATE OR REPLACE VIEW analytics.maintenance_by_type AS
SELECT
    maintenance_type,
    COUNT(*) AS records,
    SUM(parts_cost + labor_cost) AS total_cost,
    SUM(downtime_hours) AS downtime_hours,
    AVG(downtime_hours) AS avg_downtime_hours,
    AVG(odometer_reading) AS avg_odometer_reading
FROM gold.fact_maintenance
GROUP BY maintenance_type;

CREATE OR REPLACE VIEW analytics.truck_scorecard AS
WITH trip_stats AS (
    SELECT
        trip.truck_sk AS truck_sk,
        COUNT(*) AS trips,
        SUM(trip.actual_distance_miles) AS miles,
        SUM(trip.fuel_gallons_used) AS gallons,
        SUM(trip.idle_time_hours) AS idle_hours
    FROM gold.fact_trips AS trip
    WHERE trip.truck_sk <> -1
    GROUP BY trip.truck_sk
),
incident_stats AS (
    SELECT
        incident.truck_sk AS truck_sk,
        COUNT(*) AS incidents,
        COALESCE(SUM(incident.claim_amount), 0) AS claims
    FROM gold.fact_safety_incidents AS incident
    WHERE incident.truck_sk <> -1
    GROUP BY incident.truck_sk
),
upkeep_stats AS (
    SELECT
        maint.truck_sk AS truck_sk,
        COUNT(*) AS maintenance_records,
        SUM(maint.parts_cost + maint.labor_cost) AS maintenance_cost,
        SUM(maint.downtime_hours) AS downtime_hours
    FROM gold.fact_maintenance AS maint
    WHERE maint.truck_sk <> -1
    GROUP BY maint.truck_sk
)
SELECT
    truck.unit_number AS unit_number,
    truck.make AS make,
    truck.model_year AS model_year,
    truck.status AS truck_status,
    COALESCE(trip_stats.trips, 0) AS trips,
    COALESCE(trip_stats.miles, 0) AS miles,
    CASE
        WHEN COALESCE(trip_stats.gallons, 0) > 0
        THEN trip_stats.miles / trip_stats.gallons
    END AS mpg,
    COALESCE(incident_stats.incidents, 0) AS incidents,
    COALESCE(incident_stats.claims, 0) AS claims,
    COALESCE(upkeep_stats.maintenance_records, 0) AS maintenance_records,
    COALESCE(upkeep_stats.maintenance_cost, 0) AS maintenance_cost,
    COALESCE(upkeep_stats.downtime_hours, 0) AS downtime_hours
FROM gold.dim_truck AS truck
LEFT JOIN trip_stats ON trip_stats.truck_sk = truck.truck_sk
LEFT JOIN incident_stats ON incident_stats.truck_sk = truck.truck_sk
LEFT JOIN upkeep_stats ON upkeep_stats.truck_sk = truck.truck_sk
WHERE truck.truck_sk <> -1;

CREATE OR REPLACE VIEW analytics.incidents_by_location AS
SELECT
    location_city AS city,
    location_state AS state,
    COUNT(*) AS incidents,
    COALESCE(SUM(claim_amount), 0) AS claims,
    SUM(CASE WHEN injury_flag THEN 1 ELSE 0 END) AS injuries,
    SUM(CASE WHEN preventable_flag THEN 1 ELSE 0 END) AS preventable,
    SUM(CASE WHEN at_fault_flag THEN 1 ELSE 0 END) AS at_fault
FROM gold.fact_safety_incidents
GROUP BY
    location_city,
    location_state;

CREATE OR REPLACE VIEW analytics.safety_monthly AS
SELECT
    dated.year_number AS year_number,
    dated.month_number AS month_number,
    dated.month_name AS month_name,
    COUNT(*) AS incidents,
    SUM(CASE WHEN incident.at_fault_flag THEN 1 ELSE 0 END) AS at_fault,
    SUM(CASE WHEN incident.preventable_flag THEN 1 ELSE 0 END) AS preventable,
    SUM(CASE WHEN incident.injury_flag THEN 1 ELSE 0 END) AS injuries,
    COALESCE(SUM(incident.claim_amount), 0) AS claims
FROM gold.fact_safety_incidents AS incident
INNER JOIN gold.dim_date AS dated ON dated.date_key = incident.incident_date_key
GROUP BY
    dated.year_number,
    dated.month_number,
    dated.month_name;

CREATE OR REPLACE VIEW analytics.facility_safety AS
SELECT
    fac.facility_name AS facility_name,
    fac.city AS city,
    fac.state AS state,
    COUNT(*) AS delivery_events,
    AVG(CASE WHEN ev.on_time_flag THEN 1.0 ELSE 0.0 END) AS delivery_on_time_rate,
    COALESCE(SUM(incident.incidents), 0) AS nearby_incidents
FROM gold.fact_delivery_events AS ev
INNER JOIN gold.dim_facility AS fac ON fac.facility_sk = ev.facility_sk
LEFT JOIN (
    SELECT
        location_city AS city,
        location_state AS state,
        COUNT(*) AS incidents
    FROM gold.fact_safety_incidents
    GROUP BY
        location_city,
        location_state
) AS incident
    ON incident.city = fac.city
    AND incident.state = fac.state
WHERE ev.event_type = 'DELIVERY'
GROUP BY
    fac.facility_name,
    fac.city,
    fac.state;

GRANT SELECT ON analytics.maintenance_monthly TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.maintenance_by_type TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.truck_scorecard TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.incidents_by_location TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.safety_monthly TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.facility_safety TO analyst_ro, dashboard_ro, dq_runner;

COMMIT;
