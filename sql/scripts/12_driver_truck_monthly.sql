-- Monthly driver and truck marts matching data/marts grains.
BEGIN;

CREATE OR REPLACE VIEW analytics.driver_monthly AS
WITH trip_month AS (
    SELECT
        trip.driver_sk AS driver_sk,
        dated.year_number AS year_number,
        dated.month_number AS month_number,
        dated.month_name AS month_name,
        COUNT(*) AS trips,
        SUM(trip.actual_distance_miles) AS miles,
        SUM(trip.fuel_gallons_used) AS gallons,
        AVG(trip.idle_time_hours) AS avg_idle_hours,
        SUM(load.revenue) AS revenue
    FROM gold.fact_trips AS trip
    INNER JOIN gold.dim_date AS dated ON dated.date_key = trip.dispatch_date_key
    LEFT JOIN gold.fact_loads AS load ON load.load_id = trip.load_id
    WHERE trip.driver_sk <> -1
    GROUP BY
        trip.driver_sk,
        dated.year_number,
        dated.month_number,
        dated.month_name
),
punctuality AS (
    SELECT
        trip.driver_sk AS driver_sk,
        dated.year_number AS year_number,
        dated.month_number AS month_number,
        AVG(CASE WHEN ev.on_time_flag THEN 1.0 ELSE 0.0 END) AS on_time_rate
    FROM gold.fact_delivery_events AS ev
    INNER JOIN gold.fact_trips AS trip ON trip.trip_id = ev.trip_id
    INNER JOIN gold.dim_date AS dated ON dated.date_key = trip.dispatch_date_key
    WHERE ev.event_type = 'DELIVERY'
    AND trip.driver_sk <> -1
    GROUP BY
        trip.driver_sk,
        dated.year_number,
        dated.month_number
)
SELECT
    drv.driver_id AS driver_id,
    drv.first_name || ' ' || drv.last_name AS driver_name,
    MAKE_DATE(trip_month.year_number, trip_month.month_number, 1) AS month_start,
    trip_month.trips AS trips_completed,
    trip_month.miles AS total_miles,
    trip_month.revenue AS total_revenue,
    CASE
        WHEN trip_month.gallons > 0
        THEN trip_month.miles / trip_month.gallons
    END AS average_mpg,
    trip_month.gallons AS total_fuel_gallons,
    punctuality.on_time_rate AS on_time_delivery_rate,
    trip_month.avg_idle_hours AS average_idle_hours
FROM trip_month
INNER JOIN gold.dim_driver AS drv ON drv.driver_sk = trip_month.driver_sk
LEFT JOIN punctuality
    ON punctuality.driver_sk = trip_month.driver_sk
    AND punctuality.year_number = trip_month.year_number
    AND punctuality.month_number = trip_month.month_number;

CREATE OR REPLACE VIEW analytics.truck_monthly AS
WITH trip_month AS (
    SELECT
        trip.truck_sk AS truck_sk,
        dated.year_number AS year_number,
        dated.month_number AS month_number,
        dated.month_name AS month_name,
        COUNT(*) AS trips,
        SUM(trip.actual_distance_miles) AS miles,
        SUM(trip.fuel_gallons_used) AS gallons,
        SUM(load.revenue) AS revenue
    FROM gold.fact_trips AS trip
    INNER JOIN gold.dim_date AS dated ON dated.date_key = trip.dispatch_date_key
    LEFT JOIN gold.fact_loads AS load ON load.load_id = trip.load_id
    WHERE trip.truck_sk <> -1
    GROUP BY
        trip.truck_sk,
        dated.year_number,
        dated.month_number,
        dated.month_name
),
upkeep_month AS (
    SELECT
        maint.truck_sk AS truck_sk,
        dated.year_number AS year_number,
        dated.month_number AS month_number,
        COUNT(*) AS maintenance_events,
        SUM(maint.parts_cost + maint.labor_cost) AS maintenance_cost,
        SUM(maint.downtime_hours) AS downtime_hours
    FROM gold.fact_maintenance AS maint
    INNER JOIN gold.dim_date AS dated ON dated.date_key = maint.maintenance_date_key
    WHERE maint.truck_sk <> -1
    GROUP BY
        maint.truck_sk,
        dated.year_number,
        dated.month_number
),
months AS (
    SELECT truck_sk, year_number, month_number FROM trip_month
    UNION
    SELECT truck_sk, year_number, month_number FROM upkeep_month
)
SELECT
    truck.truck_id AS truck_id,
    truck.unit_number AS unit_number,
    MAKE_DATE(months.year_number, months.month_number, 1) AS month_start,
    COALESCE(trip_month.trips, 0) AS trips_completed,
    COALESCE(trip_month.miles, 0) AS total_miles,
    trip_month.revenue AS total_revenue,
    CASE
        WHEN COALESCE(trip_month.gallons, 0) > 0
        THEN trip_month.miles / trip_month.gallons
    END AS average_mpg,
    COALESCE(upkeep_month.maintenance_events, 0) AS maintenance_events,
    COALESCE(upkeep_month.maintenance_cost, 0) AS maintenance_cost,
    COALESCE(upkeep_month.downtime_hours, 0) AS downtime_hours,
    COALESCE(trip_month.trips, 0) / EXTRACT(
        DAY FROM (
            MAKE_DATE(months.year_number, months.month_number, 1)
            + INTERVAL '1 month' - INTERVAL '1 day'
        )
    ) AS utilization_rate
FROM months
INNER JOIN gold.dim_truck AS truck ON truck.truck_sk = months.truck_sk
LEFT JOIN trip_month
    ON trip_month.truck_sk = months.truck_sk
    AND trip_month.year_number = months.year_number
    AND trip_month.month_number = months.month_number
LEFT JOIN upkeep_month
    ON upkeep_month.truck_sk = months.truck_sk
    AND upkeep_month.year_number = months.year_number
    AND upkeep_month.month_number = months.month_number;

GRANT SELECT ON analytics.driver_monthly TO analyst_ro, dashboard_ro, dq_runner;
GRANT SELECT ON analytics.truck_monthly TO analyst_ro, dashboard_ro, dq_runner;

COMMIT;
