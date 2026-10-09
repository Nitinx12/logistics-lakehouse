# Validates the silver.load_customers procedure and its output.
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
    / "proc_silver_load_customers.sql"
)


def _silver_cursor() -> object:
    from src.utils.connection import get_postgres_connection

    try:
        connection = get_postgres_connection()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unreachable: {exc}")
    return connection


def test_procedure_file_defines_load_customers() -> None:
    text = PROC_FILE.read_text(encoding="utf-8")
    assert "silver.load_customers" in text
    assert "silver.customers" in text
    assert "dq.quarantine_customers" in text


def test_silver_customers_holds_all_bronze_rows() -> None:
    connection = _silver_cursor()
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver.customers")
            silver = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM bronze.customers")
            bronze = cur.fetchone()
            assert silver is not None and bronze is not None
            assert silver[0] == 200 and silver[0] == bronze[0]
    finally:
        connection.close()


def test_silver_customer_keys_unique_and_not_null() -> None:
    connection = _silver_cursor()
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM silver.customers WHERE customer_id IS NULL"
            )
            nulls = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM (SELECT customer_id FROM silver.customers "
                "GROUP BY customer_id HAVING COUNT(*) > 1) AS dupes"
            )
            dupes = cur.fetchone()
            assert nulls is not None and nulls[0] == 0
            assert dupes is not None and dupes[0] == 0
    finally:
        connection.close()


def test_silver_customer_columns_typed() -> None:
    connection = _silver_cursor()
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'silver' AND table_name = 'customers' "
                "AND column_name IN ('credit_terms_days', 'contract_start_date', "
                "'annual_revenue_potential', 'loaded_at')"
            )
            found = {row[0]: row[1] for row in cur.fetchall()}
            assert found.get("credit_terms_days") == "bigint"
            assert found.get("contract_start_date") == "date"
            assert found.get("annual_revenue_potential") == "bigint"
            assert found.get("loaded_at") == "timestamp with time zone"
    finally:
        connection.close()


def test_silver_customer_amounts_non_negative() -> None:
    connection = _silver_cursor()
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM silver.customers "
                "WHERE credit_terms_days < 0 OR annual_revenue_potential < 0"
            )
            row = cur.fetchone()
            assert row is not None and row[0] == 0
    finally:
        connection.close()


def test_silver_customers_load_logged_success() -> None:
    connection = _silver_cursor()
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM silver.etl_logs "
                "WHERE procedure_name = 'silver.load_customers' "
                "AND target_table = 'silver.customers' AND status = 'SUCCESS'"
            )
            row = cur.fetchone()
            assert row is not None and row[0] >= 1
    finally:
        connection.close()
