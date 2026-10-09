# Validates bronze contracts, extract helpers, and live bronze tables.
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from src.jobs.extract.mongo.mongo_to_postgres import (
    build_pipeline,
    detect_incremental_column,
    format_mongo_bound,
    is_retryable_error,
    spark_type_to_postgres,
    to_utc,
    watermark_range_filter,
)

BRONZE_TABLES = (
    "delivery_events",
    "maintenance_records",
    "safety_incidents",
)

BUSINESS_KEYS = {
    "delivery_events": "event_id",
    "maintenance_records": "maintenance_id",
    "safety_incidents": "incident_id",
}


def _contract_path(table: str) -> Path:
    return REPO_ROOT / "contracts" / "mongo" / f"{table}.yaml"


def _csv_path(table: str) -> Path:
    return REPO_ROOT / "data" / "fleet_operations" / f"{table}.csv"


def _contract_columns(table: str) -> list[str]:
    text = _contract_path(table).read_text(encoding="utf-8")
    return re.findall(r"name:\s*([A-Za-z_][A-Za-z0-9_]*)", text)


def _contract_key(table: str) -> str:
    text = _contract_path(table).read_text(encoding="utf-8")
    match = re.search(r"key:\s*\[([A-Za-z_][A-Za-z0-9_]*)\]", text)
    assert match is not None
    return match.group(1)


def _contract_rows_observed(table: str) -> int:
    text = _contract_path(table).read_text(encoding="utf-8")
    match = re.search(r"rows_observed:\s*(\d+)", text)
    assert match is not None
    return int(match.group(1))


def _csv_header(table: str) -> list[str]:
    with _csv_path(table).open(encoding="utf-8") as handle:
        return [part.strip() for part in handle.readline().strip().split(",")]


def _csv_row_count(table: str) -> int:
    with _csv_path(table).open(encoding="utf-8") as handle:
        next(handle)
        return sum(1 for _ in handle)


def _bronze_cursor() -> object:
    from src.utils.connection import get_postgres_connection

    try:
        connection = get_postgres_connection()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unreachable: {exc}")
    return connection


def test_mongo_contracts_cover_all_bronze_tables() -> None:
    for table in BRONZE_TABLES:
        assert _contract_path(table).exists()


def test_contract_key_matches_known_business_key() -> None:
    for table in BRONZE_TABLES:
        assert _contract_key(table) == BUSINESS_KEYS[table]


def test_contract_declares_loaded_at_column() -> None:
    for table in BRONZE_TABLES:
        assert "loaded_at" in _contract_columns(table)


def test_contract_columns_match_csv_headers() -> None:
    for table in BRONZE_TABLES:
        assert sorted(_contract_columns(table)) == sorted(_csv_header(table))


def test_csv_row_counts_match_rows_observed() -> None:
    for table in BRONZE_TABLES:
        assert _csv_row_count(table) == _contract_rows_observed(table)


def test_build_pipeline_first_chunk_is_open_at_start() -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 2, tzinfo=UTC)
    pipeline = build_pipeline("loaded_at", start, end, True, kind="string")
    assert '"$gt"' in pipeline
    assert '"$lte"' in pipeline


def test_build_pipeline_middle_chunk_excludes_upper_bound() -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 2, tzinfo=UTC)
    pipeline = build_pipeline(
        "loaded_at", start, end, False, last_chunk=False, kind="string"
    )
    assert '"$gte"' in pipeline
    assert '"$lt"' in pipeline


def test_build_pipeline_without_watermark_matches_everything() -> None:
    end = datetime(2024, 1, 2, tzinfo=UTC)
    assert build_pipeline(None, None, end, True) == "[]"


def test_watermark_range_filter_incremental_window() -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 2, tzinfo=UTC)
    assert watermark_range_filter("loaded_at", start, end, "date") == {
        "loaded_at": {"$gt": start, "$lte": end}
    }


def test_watermark_range_filter_full_load_has_only_upper_bound() -> None:
    end = datetime(2024, 1, 2, tzinfo=UTC)
    assert watermark_range_filter("loaded_at", None, end, "date") == {
        "loaded_at": {"$lte": end}
    }


def test_detect_incremental_column_honors_override() -> None:
    fields = ["loaded_at", "updated_at"]
    assert detect_incremental_column(fields, "loaded_at") == "loaded_at"


def test_detect_incremental_column_rejects_unknown_override() -> None:
    assert detect_incremental_column(["loaded_at"], "missing_col") is None


def test_detect_incremental_column_autodetects_candidate() -> None:
    assert detect_incremental_column(["_id", "created_at"], None) == "created_at"


def test_spark_type_to_postgres_maps_common_types() -> None:
    assert spark_type_to_postgres("string") == "TEXT"
    assert spark_type_to_postgres("long") == "BIGINT"
    assert spark_type_to_postgres("double") == "DOUBLE PRECISION"
    assert spark_type_to_postgres("timestamp") == "TIMESTAMPTZ"
    assert spark_type_to_postgres("date") == "DATE"


def test_spark_type_to_postgres_falls_back_to_text() -> None:
    assert spark_type_to_postgres("mystery_type") == "TEXT"


def test_is_retryable_error_marks_fatal_spark_errors() -> None:
    assert (
        is_retryable_error(RuntimeError("NoSuchMethodError on resolveAndBind")) is False
    )
    assert is_retryable_error(RuntimeError("connection reset by peer")) is True


def test_format_mongo_bound_renders_string_watermark() -> None:
    value = datetime(2024, 1, 1, 12, 30, tzinfo=UTC)
    assert format_mongo_bound(value, "string") == '"2024-01-01 12:30:00"'


def test_to_utc_parses_wall_time_as_utc() -> None:
    assert to_utc("2024-01-01 12:30:00") == datetime(2024, 1, 1, 12, 30, tzinfo=UTC)


def test_bronze_tables_have_rows() -> None:
    connection = _bronze_cursor()
    try:
        with connection.cursor() as cur:
            for table in BRONZE_TABLES:
                cur.execute(f'SELECT COUNT(*) FROM bronze."{table}"')
                row = cur.fetchone()
                assert row is not None and row[0] > 0
    finally:
        connection.close()


def test_bronze_loaded_at_has_no_nulls() -> None:
    connection = _bronze_cursor()
    try:
        with connection.cursor() as cur:
            for table in BRONZE_TABLES:
                cur.execute(
                    f'SELECT COUNT(*) FROM bronze."{table}" WHERE loaded_at IS NULL'
                )
                row = cur.fetchone()
                assert row is not None and row[0] == 0
    finally:
        connection.close()


def test_bronze_business_keys_unique_and_not_null() -> None:
    connection = _bronze_cursor()
    try:
        with connection.cursor() as cur:
            for table in BRONZE_TABLES:
                key = BUSINESS_KEYS[table]
                cur.execute(
                    f'SELECT COUNT(*) FROM bronze."{table}" WHERE "{key}" IS NULL'
                )
                nulls = cur.fetchone()
                cur.execute(
                    f'SELECT COUNT(*) FROM (SELECT "{key}" FROM bronze."{table}" '
                    f'GROUP BY "{key}" HAVING COUNT(*) > 1) AS dupes'
                )
                dupes = cur.fetchone()
                assert nulls is not None and nulls[0] == 0
                assert dupes is not None and dupes[0] == 0
    finally:
        connection.close()
