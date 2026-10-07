# Analytical queries backing the Streamlit tabs; analytics marts only.
QUERIES: dict[str, str] = {
    "fleet_kpis": """
SELECT
    truck_count,
    trailer_count,
    driver_count,
    lifetime_miles,
    lifetime_gallons,
    lifetime_fuel_cost,
    fleet_avg_mpg
FROM analytics.fleet_kpis
""",
    "fuel_trend_monthly": """
SELECT
    year_number,
    month_number,
    BTRIM(month_name) AS month_name,
    purchases,
    gallons,
    total_cost,
    avg_price_per_gallon
FROM analytics.fuel_monthly
ORDER BY
    year_number,
    month_number
""",
    "mpg_by_truck": """
SELECT
    unit_number,
    make,
    trips,
    miles,
    gallons,
    mpg
FROM analytics.truck_efficiency
ORDER BY mpg
""",
    "idle_hours_by_truck": """
SELECT
    unit_number,
    trips,
    idle_hours,
    miles
FROM analytics.truck_efficiency
ORDER BY idle_hours DESC
""",
    "load_kpis": """
SELECT
    loads,
    revenue,
    weight_lbs,
    avg_revenue_per_load,
    delivery_on_time_rate
FROM analytics.load_kpis
""",
    "revenue_trend_monthly": """
SELECT
    year_number,
    month_number,
    BTRIM(month_name) AS month_name,
    loads,
    revenue,
    delivery_on_time_rate
FROM analytics.revenue_monthly
ORDER BY
    year_number,
    month_number
""",
    "revenue_by_customer": """
SELECT
    customer_name,
    customer_type,
    loads,
    revenue,
    revenue_share
FROM analytics.revenue_by_customer
ORDER BY revenue DESC
""",
    "delays_by_facility": """
SELECT
    facility_name,
    city,
    state,
    delivery_events,
    avg_delay_minutes,
    avg_detention_minutes,
    delivery_on_time_rate
FROM analytics.facility_delays
ORDER BY avg_delay_minutes DESC
""",
    "route_performance": """
SELECT
    route_name,
    trips,
    avg_actual_hours,
    typical_hours,
    avg_delay_minutes
FROM analytics.route_performance
ORDER BY trips DESC
""",
    "safety_kpis": """
SELECT
    incidents,
    total_claims,
    preventable_rate,
    at_fault_rate,
    injuries,
    maintenance_cost,
    downtime_hours
FROM analytics.safety_kpis
""",
    "claims_by_type": """
SELECT
    incident_type,
    incidents,
    claims,
    preventable,
    injuries
FROM analytics.claims_by_type
ORDER BY claims DESC
""",
    "maintenance_by_truck": """
SELECT
    unit_number,
    records,
    cost,
    downtime_hours
FROM analytics.maintenance_by_truck
ORDER BY cost DESC
""",
    "incidents_trend_monthly": """
SELECT
    year_number,
    month_number,
    BTRIM(month_name) AS month_name,
    incidents,
    claims
FROM analytics.incidents_monthly
ORDER BY
    year_number,
    month_number
""",
    "monthly_pnl": """
SELECT
    year_number,
    month_number,
    BTRIM(month_name) AS month_name,
    revenue,
    fuel_cost,
    maintenance_cost,
    claim_cost,
    detention_hours,
    gross_margin,
    gross_margin_rate
FROM analytics.monthly_pnl
ORDER BY
    year_number,
    month_number
""",
    "driver_scorecard": """
SELECT
    driver_name,
    employment_status,
    home_terminal,
    trips,
    miles,
    mpg,
    idle_hours,
    incidents,
    at_fault,
    claims
FROM analytics.driver_scorecard
ORDER BY miles DESC
""",
    "load_mix": """
SELECT
    load_type,
    load_status,
    booking_type,
    loads,
    revenue,
    weight_lbs
FROM analytics.load_mix
ORDER BY revenue DESC
""",
    "delay_buckets": """
SELECT
    delay_bucket,
    delivery_events,
    event_share,
    avg_detention_minutes
FROM analytics.delay_buckets
""",
    "customer_monthly": """
SELECT
    customer_name,
    customer_type,
    year_number,
    month_number,
    BTRIM(month_name) AS month_name,
    loads,
    revenue
FROM analytics.customer_monthly
ORDER BY
    customer_name,
    year_number,
    month_number
""",
    "facility_monthly": """
SELECT
    facility_name,
    city,
    state,
    year_number,
    month_number,
    delivery_events,
    avg_delay_minutes,
    delivery_on_time_rate
FROM analytics.facility_monthly
ORDER BY
    facility_name,
    year_number,
    month_number
""",
    "maintenance_monthly": """
SELECT
    year_number,
    month_number,
    BTRIM(month_name) AS month_name,
    records,
    parts_cost,
    labor_cost,
    total_cost,
    downtime_hours,
    avg_odometer_reading
FROM analytics.maintenance_monthly
ORDER BY
    year_number,
    month_number
""",
    "maintenance_by_type": """
SELECT
    maintenance_type,
    records,
    total_cost,
    downtime_hours,
    avg_downtime_hours,
    avg_odometer_reading
FROM analytics.maintenance_by_type
ORDER BY total_cost DESC
""",
    "truck_scorecard": """
SELECT
    unit_number,
    make,
    model_year,
    truck_status,
    trips,
    miles,
    mpg,
    incidents,
    claims,
    maintenance_records,
    maintenance_cost,
    downtime_hours
FROM analytics.truck_scorecard
ORDER BY miles DESC
""",
    "incidents_by_location": """
SELECT
    city,
    state,
    incidents,
    claims,
    injuries,
    preventable,
    at_fault
FROM analytics.incidents_by_location
ORDER BY incidents DESC
""",
    "safety_monthly": """
SELECT
    year_number,
    month_number,
    BTRIM(month_name) AS month_name,
    incidents,
    at_fault,
    preventable,
    injuries,
    claims
FROM analytics.safety_monthly
ORDER BY
    year_number,
    month_number
""",
    "facility_safety": """
SELECT
    facility_name,
    city,
    state,
    delivery_events,
    delivery_on_time_rate,
    nearby_incidents
FROM analytics.facility_safety
ORDER BY nearby_incidents DESC
""",
    "driver_monthly": """
SELECT
    driver_id,
    driver_name,
    month_start,
    trips_completed,
    total_miles,
    total_revenue,
    average_mpg,
    total_fuel_gallons,
    on_time_delivery_rate,
    average_idle_hours
FROM analytics.driver_monthly
ORDER BY
    driver_id,
    month_start
""",
    "truck_monthly": """
SELECT
    truck_id,
    unit_number,
    month_start,
    trips_completed,
    total_miles,
    total_revenue,
    average_mpg,
    maintenance_events,
    maintenance_cost,
    downtime_hours,
    utilization_rate
FROM analytics.truck_monthly
ORDER BY
    truck_id,
    month_start
""",
}
