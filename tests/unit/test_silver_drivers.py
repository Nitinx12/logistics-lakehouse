# Validates the silver.load_drivers procedure and its output.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

PROC_FILE = (
    REPO_ROOT
    / "src"
    / "jobs"
    / "transform"
    / "databricks"
    / "proc_silver_load_drivers.sql"
)


def _silver_cursor() -> object:
    from src.utils.connection import get_postgres_connection

    try:
        connection = get_postgres_connection()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unreachable: {exc}")
    return connection


def test_procedure_file_defines_load_drivers() -> None:
    text = PROC_FILE.read_text(encoding="utf-8")
    assert "silver.load_drivers" in text
    assert "silver.drivers" in text
    assert "dq.quarantine_drivers" in text


def test_silver_drivers_holds_all_bronze_rows() -> None:
    connection = _silver_cursor()
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver.drivers")
            silver = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM bronze.drivers")
            bronze = cur.fetchone()
            assert silver is not None and bronze is not None
            assert silver[0] == 150 and silver[0] == bronze[0]
    finally:
        connection.close()


def test_silver_driver_keys_unique_and_not_null() -> None:
    connection = _silver_cursor()
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver.drivers WHERE driver_id IS NULL")
            nulls = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM (SELECT driver_id FROM silver.drivers "
                "GROUP BY driver_id HAVING COUNT(*) > 1) AS dupes"
            )
            dupes = cur.fetchone()
            assert nulls is not None and nulls[0] == 0
            assert dupes is not None and dupes[0] == 0
    finally:
        connection.close()


def test_silver_drivers_keeps_active_drivers_null_terminated() -> None:
    connection = _silver_cursor()
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM silver.drivers WHERE termination_date IS NULL"
            )
            row = cur.fetchone()
            assert row is not None and row[0] == 124
    finally:
        connection.close()


def test_silver_drivers_dates_ordered() -> None:
    connection = _silver_cursor()
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM silver.drivers "
                "WHERE termination_date IS NOT NULL "
                "AND termination_date < hire_date"
            )
            row = cur.fetchone()
            assert row is not None and row[0] == 0
    finally:
        connection.close()


def test_silver_drivers_load_logged_success() -> None:
    connection = _silver_cursor()
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM silver.etl_logs "
                "WHERE procedure_name = 'silver.load_drivers' "
                "AND target_table = 'silver.drivers' AND status = 'SUCCESS'"
            )
            row = cur.fetchone()
            assert row is not None and row[0] >= 1
    finally:
        connection.close()
