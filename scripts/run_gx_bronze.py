# Validates bronze tables against Great Expectations suites.
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
CHECKPOINT_NAME = "bronze"
CONNECTION_TEMPLATE = (
    "postgresql+psycopg://${POSTGRES_USER}:${POSTGRES_PASSWORD}"
    "@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
)

TABLES: dict[str, dict[str, Any]] = {
    "customers": {
        "key": "customer_id",
        "min_rows": 200,
        "columns": [
            "customer_id",
            "customer_name",
            "customer_type",
            "credit_terms_days",
            "primary_freight_type",
            "account_status",
            "contract_start_date",
            "annual_revenue_potential",
            "loaded_at",
            "_loaded_at",
        ],
    },
    "drivers": {
        "key": "driver_id",
        "min_rows": 150,
        "columns": [
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
            "loaded_at",
            "_loaded_at",
        ],
    },
    "facilities": {
        "key": "facility_id",
        "min_rows": 50,
        "columns": [
            "facility_id",
            "facility_name",
            "facility_type",
            "city",
            "state",
            "latitude",
            "longitude",
            "dock_doors",
            "operating_hours",
            "loaded_at",
            "_loaded_at",
        ],
    },
    "fuel_purchases": {
        "key": "fuel_purchase_id",
        "min_rows": 196442,
        "columns": [
            "fuel_purchase_id",
            "trip_id",
            "truck_id",
            "driver_id",
            "purchase_date",
            "location_city",
            "location_state",
            "gallons",
            "price_per_gallon",
            "total_cost",
            "fuel_card_number",
            "loaded_at",
            "_loaded_at",
        ],
    },
    "loads": {
        "key": "load_id",
        "min_rows": 85410,
        "columns": [
            "load_id",
            "customer_id",
            "route_id",
            "load_date",
            "load_type",
            "weight_lbs",
            "pieces",
            "revenue",
            "fuel_surcharge",
            "accessorial_charges",
            "load_status",
            "booking_type",
            "loaded_at",
            "_loaded_at",
        ],
    },
    "routes": {
        "key": "route_id",
        "min_rows": 58,
        "columns": [
            "route_id",
            "origin_city",
            "origin_state",
            "destination_city",
            "destination_state",
            "typical_distance_miles",
            "base_rate_per_mile",
            "fuel_surcharge_rate",
            "typical_transit_days",
            "loaded_at",
            "_loaded_at",
        ],
    },
    "trailers": {
        "key": "trailer_id",
        "min_rows": 180,
        "columns": [
            "trailer_id",
            "trailer_number",
            "trailer_type",
            "length_feet",
            "model_year",
            "vin",
            "acquisition_date",
            "status",
            "current_location",
            "loaded_at",
            "_loaded_at",
        ],
    },
    "trips": {
        "key": "trip_id",
        "min_rows": 85410,
        "columns": [
            "trip_id",
            "load_id",
            "driver_id",
            "truck_id",
            "trailer_id",
            "dispatch_date",
            "actual_distance_miles",
            "actual_duration_hours",
            "fuel_gallons_used",
            "average_mpg",
            "idle_time_hours",
            "trip_status",
            "loaded_at",
            "_loaded_at",
        ],
    },
    "trucks": {
        "key": "truck_id",
        "min_rows": 120,
        "columns": [
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
            "loaded_at",
            "_loaded_at",
        ],
    },
    "delivery_events": {
        "key": "event_id",
        "min_rows": 170820,
        "columns": [
            "actual_datetime",
            "detention_minutes",
            "event_id",
            "event_type",
            "facility_id",
            "load_id",
            "loaded_at",
            "location_city",
            "location_state",
            "on_time_flag",
            "scheduled_datetime",
            "trip_id",
            "id",
            "_loaded_at",
        ],
    },
    "maintenance_records": {
        "key": "maintenance_id",
        "min_rows": 2920,
        "columns": [
            "downtime_hours",
            "facility_location",
            "labor_cost",
            "labor_hours",
            "loaded_at",
            "maintenance_date",
            "maintenance_id",
            "maintenance_type",
            "odometer_reading",
            "parts_cost",
            "service_description",
            "total_cost",
            "truck_id",
            "id",
            "_loaded_at",
        ],
    },
    "safety_incidents": {
        "key": "incident_id",
        "min_rows": 170,
        "columns": [
            "at_fault_flag",
            "cargo_damage_cost",
            "claim_amount",
            "description",
            "driver_id",
            "incident_date",
            "incident_id",
            "incident_type",
            "injury_flag",
            "loaded_at",
            "location_city",
            "location_state",
            "preventable_flag",
            "trip_id",
            "truck_id",
            "vehicle_damage_cost",
            "id",
            "_loaded_at",
        ],
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
            schema_name="bronze",
        )
    try:
        return asset.add_batch_definition_whole_table(name=f"{table}_full")
    except Exception:  # noqa: BLE001
        return asset.get_batch_definition(f"{table}_full")


def _build_suite(table: str, spec: dict[str, Any]) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=f"bronze.{table}")
    suite.add_expectation(
        gxe.ExpectTableRowCountToBeBetween(min_value=spec["min_rows"])
    )
    suite.add_expectation(
        gxe.ExpectTableColumnsToMatchOrderedList(column_list=spec["columns"])
    )
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=spec["key"]))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column=spec["key"]))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="loaded_at"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(
            column="loaded_at",
            regex=r"^\d{4}-\d{2}-\d{2}",
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
                name=f"bronze_{table}",
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
