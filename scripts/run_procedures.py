# Installs silver and gold procedures from src/jobs on every run.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from psycopg import Connection

from src.utils.connection import (
    close_connection,
    get_postgres_admin_connection,
    get_postgres_connection,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

TRANSFORM_MONGO = REPO_ROOT / "src" / "jobs" / "transform" / "mongo"
TRANSFORM_DATABRICKS = REPO_ROOT / "src" / "jobs" / "transform" / "databricks"
LOAD = REPO_ROOT / "src" / "jobs" / "load"

PROCEDURE_FILES = [
    TRANSFORM_MONGO / "proc_silver_load_delivery_events.sql",
    TRANSFORM_MONGO / "proc_silver_load_maintenance_records.sql",
    TRANSFORM_MONGO / "proc_silver_load_safety_incidents.sql",
    TRANSFORM_MONGO / "proc_silver_load_mongo_all.sql",
    TRANSFORM_DATABRICKS / "proc_silver_load_customers.sql",
    TRANSFORM_DATABRICKS / "proc_silver_load_drivers.sql",
    TRANSFORM_DATABRICKS / "proc_silver_load_facilities.sql",
    TRANSFORM_DATABRICKS / "proc_silver_load_routes.sql",
    TRANSFORM_DATABRICKS / "proc_silver_load_trailers.sql",
    TRANSFORM_DATABRICKS / "proc_silver_load_trucks.sql",
    TRANSFORM_DATABRICKS / "proc_silver_load_loads.sql",
    TRANSFORM_DATABRICKS / "proc_silver_load_trips.sql",
    TRANSFORM_DATABRICKS / "proc_silver_load_fuel_purchases.sql",
    TRANSFORM_DATABRICKS / "proc_silver_load_databricks_all.sql",
    LOAD / "proc_gold_load_dim_date.sql",
    LOAD / "proc_gold_load_dim_customers.sql",
    LOAD / "proc_gold_load_dim_drivers.sql",
    LOAD / "proc_gold_load_dim_facilities.sql",
    LOAD / "proc_gold_load_dim_routes.sql",
    LOAD / "proc_gold_load_dim_trailers.sql",
    LOAD / "proc_gold_load_dim_trucks.sql",
    LOAD / "proc_gold_load_fact_loads.sql",
    LOAD / "proc_gold_load_fact_trips.sql",
    LOAD / "proc_gold_load_fact_fuel_purchases.sql",
    LOAD / "proc_gold_load_fact_delivery_events.sql",
    LOAD / "proc_gold_load_fact_maintenance.sql",
    LOAD / "proc_gold_load_fact_safety_incidents.sql",
    LOAD / "proc_gold_load_gold_all.sql",
]


def list_procedures() -> list[Path]:
    missing = [path for path in PROCEDURE_FILES if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"procedure files missing: {missing}")
    return list(PROCEDURE_FILES)


def open_procedure_connection() -> Connection:
    try:
        return get_postgres_admin_connection()
    except ConnectionError as exc:
        logger.info("admin unavailable, installing procedures as app role: %s", exc)
        return get_postgres_connection()


def main() -> int:
    paths = list_procedures()
    connection = open_procedure_connection()
    try:
        connection.autocommit = True
        applied = 0
        for path in paths:
            sql_text = path.read_text(encoding="utf-8")
            if not sql_text.strip():
                raise ValueError(f"procedure file empty: {path.name}")
            try:
                with connection.cursor() as cur:
                    cur.execute(sql_text)
            except Exception:
                connection.rollback()
                raise
            applied += 1
        logger.info("procedures done files=%d applied=%d", len(paths), applied)
        return 0
    finally:
        close_connection(connection)


if __name__ == "__main__":
    sys.exit(main())
