# Validates gold tables against Great Expectations suites.
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.batch_definition import BatchDefinition

import src.utils.connection  # noqa: F401  (loads .env for GX ${VAR} substitution)
from src.utils.logger import get_logger

logger = get_logger(__name__)

GX_ROOT = REPO_ROOT
DATASOURCE_NAME = "lakehouse_postgres"
CHECKPOINT_NAME = "gold"
CONNECTION_TEMPLATE = (
    "postgresql+psycopg://${POSTGRES_USER}:${POSTGRES_PASSWORD}"
    "@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
)
DIM_AUDIT = [
    "loaded_at",
    "gold_batch_id",
    "gold_loaded_at",
    "gold_updated_at",
]
DATE_AUDIT = ["gold_batch_id", "gold_loaded_at"]

TABLES: dict[str, dict[str, Any]] = {
    "dim_date": {
        "key": "date_key",
        "surrogate": "date_key",
        "min_rows": 7671,
        "columns": [
            "date_key",
            "full_date",
            "year_number",
            "quarter_number",
            "month_number",
            "month_name",
            "day_number",
            "day_of_week",
            "day_name",
            "is_weekend",
            *DATE_AUDIT,
        ],
        "not_null": ["gold_batch_id"],
        "non_negative": [],
        "states": [],
    },
    "dim_customer": {
        "key": "customer_id",
        "surrogate": "customer_sk",
        "min_rows": 201,
        "columns": [
            "customer_sk",
            "customer_id",
            "customer_name",
            "customer_type",
            "credit_terms_days",
            "primary_freight_type",
            "account_status",
            "contract_start_date",
            "annual_revenue_potential",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": [],
        "states": [],
    },
    "dim_driver": {
        "key": "driver_id",
        "surrogate": "driver_sk",
        "min_rows": 151,
        "columns": [
            "driver_sk",
            "driver_id",
            "first_name",
            "last_name",
            "hire_date",
            "termination_date",
            "license_number",
            "license_state",
            "date_of_birth",
            "home_terminal",
            "employment_status",
            "cdl_class",
            "years_experience",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": [],
        "states": [],
    },
    "dim_facility": {
        "key": "facility_id",
        "surrogate": "facility_sk",
        "min_rows": 51,
        "columns": [
            "facility_sk",
            "facility_id",
            "facility_name",
            "facility_type",
            "city",
            "state",
            "latitude",
            "longitude",
            "dock_doors",
            "operating_hours",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": [],
        "states": ["state"],
    },
    "dim_route": {
        "key": "route_id",
        "surrogate": "route_sk",
        "min_rows": 59,
        "columns": [
            "route_sk",
            "route_id",
            "origin_city",
            "origin_state",
            "destination_city",
            "destination_state",
            "typical_distance_miles",
            "base_rate_per_mile",
            "fuel_surcharge_rate",
            "typical_transit_days",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": [],
        "states": ["origin_state", "destination_state"],
    },
    "dim_trailer": {
        "key": "trailer_id",
        "surrogate": "trailer_sk",
        "min_rows": 181,
        "columns": [
            "trailer_sk",
            "trailer_id",
            "trailer_number",
            "trailer_type",
            "length_feet",
            "model_year",
            "vin",
            "acquisition_date",
            "status",
            "current_location",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": [],
        "states": [],
    },
    "dim_truck": {
        "key": "truck_id",
        "surrogate": "truck_sk",
        "min_rows": 121,
        "columns": [
            "truck_sk",
            "truck_id",
            "unit_number",
            "make",
            "model_year",
            "vin",
            "acquisition_date",
            "acquisition_mileage",
            "fuel_type",
            "tank_capacity_gallons",
            "status",
            "home_terminal",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": [],
        "states": [],
    },
    "fact_loads": {
        "key": "load_id",
        "surrogate": "",
        "min_rows": 85410,
        "columns": [
            "load_id",
            "customer_sk",
            "route_sk",
            "load_date_key",
            "load_type",
            "weight_lbs",
            "pieces",
            "revenue",
            "fuel_surcharge",
            "accessorial_charges",
            "load_status",
            "booking_type",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": [
            "weight_lbs",
            "pieces",
            "revenue",
            "fuel_surcharge",
            "accessorial_charges",
        ],
        "states": [],
    },
    "fact_trips": {
        "key": "trip_id",
        "surrogate": "",
        "min_rows": 85410,
        "count_equals": "fact_loads",
        "columns": [
            "trip_id",
            "load_id",
            "driver_sk",
            "truck_sk",
            "trailer_sk",
            "dispatch_date_key",
            "actual_distance_miles",
            "actual_duration_hours",
            "fuel_gallons_used",
            "average_mpg",
            "idle_time_hours",
            "trip_status",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": [
            "actual_distance_miles",
            "actual_duration_hours",
            "fuel_gallons_used",
            "average_mpg",
            "idle_time_hours",
        ],
        "states": [],
    },
    "fact_fuel_purchases": {
        "key": "fuel_purchase_id",
        "surrogate": "",
        "min_rows": 196442,
        "columns": [
            "fuel_purchase_id",
            "trip_id",
            "truck_sk",
            "driver_sk",
            "purchase_date_key",
            "gallons",
            "price_per_gallon",
            "total_cost",
            "fuel_card_number",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": ["gallons", "price_per_gallon", "total_cost"],
        "states": [],
    },
    "fact_delivery_events": {
        "key": "event_id",
        "surrogate": "",
        "min_rows": 170820,
        "columns": [
            "event_id",
            "load_id",
            "trip_id",
            "facility_sk",
            "event_type",
            "scheduled_datetime",
            "actual_datetime",
            "delay_minutes",
            "detention_minutes",
            "on_time_flag",
            "location_city",
            "location_state",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": ["detention_minutes"],
        "states": ["location_state"],
    },
    "fact_maintenance": {
        "key": "maintenance_id",
        "surrogate": "",
        "min_rows": 2920,
        "columns": [
            "maintenance_id",
            "truck_sk",
            "maintenance_date_key",
            "maintenance_type",
            "service_description",
            "facility_location",
            "odometer_reading",
            "labor_hours",
            "labor_cost",
            "parts_cost",
            "total_cost",
            "downtime_hours",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": [
            "odometer_reading",
            "labor_hours",
            "labor_cost",
            "parts_cost",
            "total_cost",
            "downtime_hours",
        ],
        "states": [],
    },
    "fact_safety_incidents": {
        "key": "incident_id",
        "surrogate": "",
        "min_rows": 170,
        "columns": [
            "incident_id",
            "trip_id",
            "truck_sk",
            "driver_sk",
            "incident_date_key",
            "incident_type",
            "location_city",
            "location_state",
            "at_fault_flag",
            "injury_flag",
            "vehicle_damage_cost",
            "cargo_damage_cost",
            "claim_amount",
            "preventable_flag",
            "description",
            *DIM_AUDIT,
        ],
        "not_null": DIM_AUDIT,
        "non_negative": [
            "vehicle_damage_cost",
            "cargo_damage_cost",
            "claim_amount",
        ],
        "states": ["location_state"],
    },
}


def _get_datasource(context: Any) -> Any:
    return context.data_sources.add_or_update_postgres(
        name=DATASOURCE_NAME,
        connection_string=CONNECTION_TEMPLATE,
    )


def _get_batch_definition(datasource: Any, table: str) -> BatchDefinition:
    if table in datasource.get_asset_names():
        asset = datasource.get_asset(table)
    else:
        asset = datasource.add_table_asset(
            name=table,
            table_name=table,
            schema_name="gold",
        )
    try:
        return asset.add_batch_definition_whole_table(name=f"{table}_full")
    except Exception:  # noqa: BLE001
        return asset.get_batch_definition(f"{table}_full")


def _build_suite(table: str, spec: dict[str, Any]) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=f"gold.{table}")
    suite.add_expectation(
        gxe.ExpectTableRowCountToBeBetween(min_value=spec["min_rows"])
    )
    if spec.get("count_equals"):
        suite.add_expectation(
            gxe.ExpectTableRowCountToEqualOtherTable(
                other_table_name=spec["count_equals"]
            )
        )
    suite.add_expectation(
        gxe.ExpectTableColumnsToMatchOrderedList(column_list=spec["columns"])
    )
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=spec["key"]))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column=spec["key"]))
    if spec["surrogate"] and spec["surrogate"] != spec["key"]:
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeUnique(column=spec["surrogate"])
        )
    for column in spec["not_null"]:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=column))
    for column in spec["non_negative"]:
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(column=column, min_value=0)
        )
    for column in spec["states"]:
        suite.add_expectation(
            gxe.ExpectColumnValuesToMatchRegex(
                column=column,
                regex=r"^[A-Z]{2}$",
            )
        )
    return suite


def main() -> int:
    context = gx.get_context(mode="file", project_root_dir=str(GX_ROOT))
    datasource = _get_datasource(context)
    definitions = []
    for table, spec in TABLES.items():
        batch_definition = _get_batch_definition(datasource, table)
        suite = context.suites.add_or_update(_build_suite(table, spec))
        definition = context.validation_definitions.add_or_update(
            gx.ValidationDefinition(
                name=f"gold_{table}",
                data=batch_definition,
                suite=suite,
            )
        )
        definitions.append(definition)
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(name=CHECKPOINT_NAME, validation_definitions=definitions)
    )
    result = checkpoint.run()
    for key, validation in (result.run_results or {}).items():
        logger.info("gx result name=%s success=%s", key, validation.success)
    logger.info("gx checkpoint success=%s", result.success)
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
