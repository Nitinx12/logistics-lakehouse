# Extracts incremental rows from Databricks into the bronze schema.
import argparse
import io
import math
import os
import re
import sys
import time
import traceback
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg.errors
from dotenv import load_dotenv
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs._report import render_report, short_error
from src.utils.connection import (
    get_databricks_connection,
    get_postgres_connection,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

JOB_NAME = "databricks_to_postgres"
STRING_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
COPY_NULL = "\\N"
INCREMENTAL_COLUMN_CANDIDATES = [
    "loaded_at",
    "updated_at",
    "created_at",
]


# reads an int env var with a fallback default
def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


# fails fast when databricks credentials are missing or still templated
def verify_databricks_config() -> None:
    host = (os.getenv("DATABRICKS_HOST") or "").strip()
    token = (os.getenv("DATABRICKS_TOKEN") or "").strip()
    http_path = (
        os.getenv("DATABRICKS_PATH") or os.getenv("DATABRICKS_HTTP_PATH") or ""
    ).strip()
    problems = []
    if not host or "<workspace>" in host:
        problems.append("DATABRICKS_HOST")
    if not token or "change_me" in token:
        problems.append("DATABRICKS_TOKEN")
    if not http_path or "<warehouse_id>" in http_path:
        problems.append("DATABRICKS_PATH")
    if problems:
        raise RuntimeError(
            "Databricks SQL warehouse is not configured "
            f"({', '.join(problems)}); copy .env.example to .env and fill them in."
        )


# identifies exceptions that cannot succeed on another attempt
def is_retryable_error(error: Exception) -> bool:
    message = str(error).lower()
    fatal_markers = (
        "invalid access token",
        "unauthorized",
        "forbidden",
        "permission denied",
        "does not exist",
        "syntax error",
    )
    return not any(marker in message for marker in fatal_markers)


# parses CLI arguments for the databricks to postgres job
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incremental Databricks to Postgres loader"
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma separated Databricks tables, empty means all",
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Ignore watermark and reload everything",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Rows per chunk, 0 means ETL_BATCH_SIZE env",
    )
    parser.add_argument(
        "--databricks-catalog", default="", help="Override DATABRICKS_CATALOG env"
    )
    parser.add_argument(
        "--databricks-schema", default="", help="Override DATABRICKS_SCHEMA env"
    )
    parser.add_argument(
        "--target-schema", default="", help="Override POSTGRES_SCHEMA_SOURCE env"
    )
    parser.add_argument(
        "--watermark-column",
        default="",
        help="Force a watermark column, empty means auto-detect per table",
    )
    parser.add_argument(
        "--merge-key",
        default="",
        help="Force a merge key column, empty means contract key per table",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover, count and plan without writing anything",
    )
    parser.add_argument(
        "--job-name", default=JOB_NAME, help="Job name recorded in source.etl_logs"
    )
    return parser.parse_args()


# backtick-quotes a databricks identifier
def qident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


# single-quote-escapes a databricks string literal
def qliteral(value: str) -> str:
    return f"'{value.replace(chr(39), chr(39) * 2)}'"


# renders a watermark bound as a databricks timestamp literal
def format_bound(value: datetime) -> str:
    aware = value if value.tzinfo else value.replace(tzinfo=UTC)
    return f"'{aware.astimezone(UTC).strftime(STRING_TS_FORMAT)}'"


# builds the watermark predicate for one chunk with mongo-style boundaries
def build_chunk_filter(
    watermark_column: str | None,
    start: datetime | None,
    end: datetime,
    first_chunk: bool,
    last_chunk: bool = True,
) -> str:
    if watermark_column is None:
        return "1 = 1"
    lower_op = ">" if first_chunk else ">="
    upper_op = "<=" if last_chunk else "<"
    parts = [f"{qident(watermark_column)} {upper_op} {format_bound(end)}"]
    if start is not None:
        parts.insert(0, f"{qident(watermark_column)} {lower_op} {format_bound(start)}")
    return " AND ".join(parts)


# splits the watermark window into even time slices for bounded chunks
def split_window(
    start: datetime | None,
    end: datetime,
    n_chunks: int,
) -> list[tuple[datetime | None, datetime]]:
    if n_chunks <= 1 or start is None:
        return [(start, end)]
    total = (end - start).total_seconds()
    step = total / n_chunks
    bounds: list[tuple[datetime | None, datetime]] = []
    for index in range(n_chunks):
        lower = start if index == 0 else start + timedelta(seconds=step * index)
        if index == n_chunks - 1:
            upper = end
        else:
            upper = start + timedelta(seconds=step * (index + 1))
        bounds.append((lower, upper))
    return bounds


# reads the merge key for a table from its contract file
def read_contract_key(table: str) -> str | None:
    path = PROJECT_ROOT / "contracts" / "databricks" / f"{table}.yaml"
    if not path.exists():
        return None
    match = re.search(
        r"key:\s*\[([A-Za-z_][A-Za-z0-9_]*)\]", path.read_text(encoding="utf-8")
    )
    return match.group(1) if match else None


# formats one databricks cell as bronze text, None stays null
def format_cell(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=UTC)
        return aware.astimezone(UTC).strftime(f"{STRING_TS_FORMAT}%z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return str(value)


# creates source.etl_logs when it does not exist
def ensure_etl_logs_table() -> None:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute("CREATE SCHEMA IF NOT EXISTS source")
            except psycopg.errors.InsufficientPrivilege:
                logger.info("schema source exists, continuing without create right")
            try:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS source.etl_logs (id BIGSERIAL PRIMARY KEY, "
                    "run_id TEXT, job_name VARCHAR NOT NULL, collection_name VARCHAR NOT NULL, "
                    "target_schema VARCHAR NOT NULL, target_table VARCHAR NOT NULL, "
                    "mode VARCHAR NOT NULL, watermark_column VARCHAR NOT NULL, "
                    "watermark_from TIMESTAMPTZ, watermark_to TIMESTAMPTZ, "
                    "rows_extracted BIGINT NOT NULL DEFAULT 0, rows_loaded BIGINT NOT NULL DEFAULT 0, "
                    "rows_inserted BIGINT NOT NULL DEFAULT 0, rows_updated BIGINT NOT NULL DEFAULT 0, "
                    "chunks INTEGER NOT NULL DEFAULT 0, status VARCHAR NOT NULL, "
                    "validation_status VARCHAR NOT NULL DEFAULT 'N/A', validation_detail TEXT, "
                    "error_message TEXT, started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                    "finished_at TIMESTAMPTZ)"
                )
            except psycopg.errors.InvalidSchemaName:
                raise RuntimeError(
                    "source.etl_logs is missing and this role cannot create it; "
                    "ask an admin to create the source schema and table"
                )
    finally:
        conn.close()


# fetches the last successful watermark for a table
def get_last_watermark(job_name: str, table: str) -> datetime | None:
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT watermark_to FROM source.etl_logs "
                "WHERE job_name = %s AND collection_name = %s AND status = 'SUCCESS' "
                "ORDER BY watermark_to DESC LIMIT 1",
                (job_name, table),
            )
            row = cur.fetchone()
            if row is None or row[0] is None:
                return None
            value: datetime = row[0]
            return value if value.tzinfo else value.replace(tzinfo=UTC)
    finally:
        conn.close()


# inserts a STARTED audit row and returns its id
def insert_log_start(
    job_name: str,
    table: str,
    target_schema: str,
    mode: str,
    watermark_column: str | None,
    watermark_from: datetime | None,
    watermark_to: datetime,
    run_id: str = "",
) -> int:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO source.etl_logs (run_id, job_name, collection_name, target_schema, "
                "target_table, mode, watermark_column, watermark_from, watermark_to, status) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'STARTED') RETURNING id",
                (
                    run_id,
                    job_name,
                    table,
                    target_schema,
                    table,
                    mode,
                    watermark_column or "",
                    watermark_from,
                    watermark_to,
                ),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        conn.close()


# marks an audit row finished with counters and validation outcome
def update_log_finish(
    log_id: int,
    status: str,
    rows_extracted: int,
    rows_loaded: int,
    chunks: int,
    error_message: str | None = None,
    inserted: int = 0,
    updated: int = 0,
    validation: str = "N/A",
    validation_detail: str | None = None,
) -> None:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE source.etl_logs SET status = %s, rows_extracted = %s, "
                "rows_loaded = %s, chunks = %s, error_message = %s, rows_inserted = %s, "
                "rows_updated = %s, validation_status = %s, validation_detail = %s, "
                "finished_at = NOW() WHERE id = %s",
                (
                    status,
                    rows_extracted,
                    rows_loaded,
                    chunks,
                    error_message,
                    inserted,
                    updated,
                    validation,
                    validation_detail,
                    log_id,
                ),
            )
    finally:
        conn.close()


# inserts a bronze STARTED audit row and returns its id
def insert_bronze_log_start(
    job_name: str,
    table: str,
    watermark_from: datetime | None,
    watermark_to: datetime,
) -> int:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for _ in range(2):
                try:
                    cur.execute(
                        "INSERT INTO bronze.etl_logs (job_name, target_table, "
                        "watermark_from, watermark_to, status) "
                        "VALUES (%s, %s, %s, %s, 'STARTED') RETURNING id",
                        (job_name, table, watermark_from, watermark_to),
                    )
                    row = cur.fetchone()
                    return int(row[0]) if row else 0
                except psycopg.errors.UniqueViolation:
                    logger.info(
                        "clearing stale bronze STARTED row job=%s table=%s",
                        job_name,
                        table,
                    )
                    cur.execute(
                        "DELETE FROM bronze.etl_logs WHERE job_name = %s "
                        "AND target_table = %s AND status = 'STARTED'",
                        (job_name, table),
                    )
            raise RuntimeError(f"could not start bronze audit row for {table!r}")
    finally:
        conn.close()


# marks a bronze audit row finished with counters and outcome
def update_bronze_log_finish(
    log_id: int,
    status: str,
    rows_extracted: int,
    rows_loaded: int,
    error_message: str | None = None,
) -> None:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bronze.etl_logs SET status = %s, rows_extracted = %s, "
                "rows_loaded = %s, error_message = %s, finished_at = NOW() "
                "WHERE id = %s",
                (status, rows_extracted, rows_loaded, error_message, log_id),
            )
    finally:
        conn.close()


# removes a bronze STARTED row when a dry run loads nothing
def cancel_bronze_log(log_id: int) -> None:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bronze.etl_logs WHERE id = %s", (log_id,))
    finally:
        conn.close()


# discovers loadable tables in the databricks catalog schema
def discover_tables(catalog: str, schema: str) -> list[str]:
    conn = get_databricks_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"SHOW TABLES IN {qident(catalog)}.{qident(schema)}")
            return sorted(str(row[1]) for row in cur.fetchall())
        finally:
            cur.close()
    finally:
        conn.close()


# lists source columns for one table in ordinal order
def describe_table(catalog: str, schema: str, table: str) -> list[str]:
    conn = get_databricks_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT column_name FROM "
                f"{qident(catalog)}.information_schema.columns "
                f"WHERE table_schema = {qliteral(schema)} "
                f"AND table_name = {qliteral(table)} "
                "ORDER BY ordinal_position"
            )
            return [str(row[0]) for row in cur.fetchall()]
        finally:
            cur.close()
    finally:
        conn.close()


# picks the watermark column: an override wins, else first candidate present
def detect_incremental_column(columns: list[str], override: str | None) -> str | None:
    if override:
        return override if override in columns else None
    for candidate in INCREMENTAL_COLUMN_CANDIDATES:
        if candidate in columns:
            return candidate
    return None


# counts source rows inside the watermark window
def count_window(
    catalog: str,
    schema: str,
    table: str,
    watermark_column: str | None,
    watermark_from: datetime | None,
    watermark_to: datetime,
) -> int:
    conn = get_databricks_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT COUNT(*) FROM {qident(catalog)}.{qident(schema)}.{qident(table)} "
                f"WHERE {build_chunk_filter(watermark_column, watermark_from, watermark_to, True)}"
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close()
    finally:
        conn.close()


# reads one watermark chunk ordered by the watermark column
def fetch_chunk(
    catalog: str,
    schema: str,
    table: str,
    columns: list[str],
    watermark_column: str | None,
    start: datetime | None,
    end: datetime,
    first_chunk: bool,
    last_chunk: bool,
    arraysize: int,
) -> tuple[list[str], list[tuple]]:
    names = ", ".join(qident(column) for column in columns)
    order = f"ORDER BY {qident(watermark_column)}" if watermark_column else ""
    conn = get_databricks_connection()
    try:
        cur = conn.cursor()
        cur.arraysize = arraysize
        try:
            cur.execute(
                f"SELECT {names} FROM {qident(catalog)}.{qident(schema)}.{qident(table)} "
                f"WHERE {build_chunk_filter(watermark_column, start, end, first_chunk, last_chunk)} "
                f"{order}"
            )
            header = [field[0] for field in (cur.description or [])] or list(columns)
            rows: list[tuple] = []
            while True:
                batch = cur.fetchmany(arraysize)
                if not batch:
                    break
                rows.extend(batch)
            return header, rows
        finally:
            cur.close()
    finally:
        conn.close()


# creates the bronze target with text columns and evolves it with new ones
def ensure_target_table(schema: str, table: str, columns: list[str]) -> None:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            except psycopg.errors.InsufficientPrivilege:
                logger.info("schema %s exists, continuing without create right", schema)
            definitions = ", ".join(
                [f'"{column}" TEXT' for column in columns]
                + ['"_loaded_at" TIMESTAMPTZ DEFAULT NOW()']
            )
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({definitions})'
            )
            for column in [*columns, "_loaded_at"]:
                pg_type = "TIMESTAMPTZ" if column == "_loaded_at" else "TEXT"
                cur.execute(
                    f'ALTER TABLE "{schema}"."{table}" '
                    f'ADD COLUMN IF NOT EXISTS "{column}" {pg_type}'
                )
    finally:
        conn.close()


# checks whether a postgres table exists
def table_exists(schema: str, table: str) -> bool:
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


# counts rows in a postgres table, zero when missing
def count_rows(schema: str, table: str) -> int:
    if not table_exists(schema, table):
        return 0
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        conn.close()


# ensures a unique index on the merge key, False means append-only fallback
def ensure_merge_key(schema: str, table: str, key: str | None) -> bool:
    if key is None:
        return False
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid "
                "JOIN pg_namespace n ON n.oid = t.relnamespace "
                "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey) "
                "WHERE n.nspname = %s AND t.relname = %s AND a.attname = %s "
                "AND i.indisunique AND i.indnatts = 1",
                (schema, table, key),
            )
            if cur.fetchone() is not None:
                return True
            cur.execute(
                f'CREATE UNIQUE INDEX IF NOT EXISTS "{table}_{key}_uidx" '
                f'ON "{schema}"."{table}" ("{key}")'
            )
            return True
    except Exception:  # noqa: BLE001
        logger.info("no unique merge key table=%s key=%s using append-only", table, key)
        return False
    finally:
        conn.close()


# copies one chunk through a temp stage, returning inserted and updated counts
def write_chunk_upsert(
    columns: list[str],
    rows: list[tuple],
    schema: str,
    table: str,
    key: str | None,
) -> tuple[int, int]:
    buffer = io.StringIO()
    for row in rows:
        cells = [format_cell(value) for value in row]
        buffer.write(
            ",".join(
                f'"{cell.replace(chr(34), chr(34) * 2)}"'
                if cell is not None
                else COPY_NULL
                for cell in cells
            )
            + "\n"
        )
    buffer.seek(0)
    target_ref = f'"{schema}"."{table}"'
    names = ", ".join(f'"{c}"' for c in columns)
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute(
                f'CREATE TEMP TABLE "{table}__stg" (LIKE {target_ref} INCLUDING DEFAULTS) '
                "ON COMMIT DROP"
            )
            with cur.copy(
                f"COPY \"{table}__stg\" ({names}) FROM STDIN WITH (FORMAT csv, NULL '{COPY_NULL}')"
            ) as copy:
                while True:
                    chunk = buffer.read(65536)
                    if not chunk:
                        break
                    copy.write(chunk)
            if key:
                updates = ", ".join(
                    f'"{c}" = EXCLUDED."{c}"' for c in columns if c != key
                )
                if updates:
                    cur.execute(
                        f"WITH upsert AS (INSERT INTO {target_ref} ({names}) "
                        f'SELECT {names} FROM "{table}__stg" '
                        f'ON CONFLICT ("{key}") DO UPDATE SET {updates} '
                        f"RETURNING (xmax = 0) AS inserted) "
                        f"SELECT count(*) FILTER (WHERE inserted), "
                        f"count(*) FILTER (WHERE NOT inserted) FROM upsert"
                    )
                    row = cur.fetchone()
                    counts = (int(row[0]), int(row[1])) if row else (0, 0)
                else:
                    cur.execute(
                        f"INSERT INTO {target_ref} ({names}) "
                        f'SELECT {names} FROM "{table}__stg" ON CONFLICT DO NOTHING'
                    )
                    counts = (max(cur.rowcount, 0), 0)
            else:
                cur.execute(
                    f'INSERT INTO {target_ref} ({names}) SELECT {names} FROM "{table}__stg"'
                )
                counts = (max(cur.rowcount, 0), 0)
        conn.commit()
        return counts
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# validates the row count after load against the expected total
def validate_table(
    target_schema: str, table: str, expected_after: int
) -> tuple[str, str]:
    actual = count_rows(target_schema, table)
    if actual == expected_after:
        return "PASS", f"{actual:,} rows confirmed in Postgres"
    return "FAIL", f"expected {expected_after:,}, found {actual:,} in Postgres"


# runs extract and load for a single table with time-windowed chunks
def run_table(
    table: str,
    catalog: str,
    schema: str,
    target_schema: str,
    watermark_override: str | None,
    merge_key_override: str | None,
    batch_size: int,
    full_load: bool,
    job_name: str,
    dry_run: bool,
    run_id: str,
) -> dict:
    started = time.time()
    now = datetime.now(UTC)
    watermark_to = now.replace(microsecond=0)
    before = count_rows(target_schema, table)
    columns = describe_table(catalog, schema, table)
    watermark_column = detect_incremental_column(columns, watermark_override)
    if watermark_override and watermark_column is None:
        raise ValueError(
            f"watermark column {watermark_override!r} not found in table {table!r}"
        )
    merge_key = merge_key_override or read_contract_key(table)
    if watermark_column is None:
        logger.info("no watermark column table=%s using full reload", table)
        mode = "full (no watermark)"
        watermark_from = None
    elif full_load:
        mode = "FULL"
        watermark_from = None
    else:
        watermark_from = get_last_watermark(job_name, table)
        mode = "INCREMENTAL" if watermark_from is not None else "FULL"
    logger.info(
        "watermark table=%s column=%s mode=%s key=%s",
        table,
        watermark_column,
        mode,
        merge_key,
    )
    log_id = insert_log_start(
        job_name,
        table,
        target_schema,
        mode,
        watermark_column,
        watermark_from,
        watermark_to,
        run_id,
    )
    bronze_id = insert_bronze_log_start(job_name, table, watermark_from, watermark_to)
    result = {
        "name": table,
        "mode": mode,
        "status": "SUCCESS",
        "watermark_column": watermark_column,
        "total": 0,
        "extracted": 0,
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "before": before,
        "after": before,
        "columns": len(columns),
        "chunks": 0,
        "seconds": 0.0,
        "validation": "N/A",
        "validation_detail": "",
        "error": None,
    }
    try:
        total = count_window(
            catalog, schema, table, watermark_column, watermark_from, watermark_to
        )
        result["total"] = total
        if total == 0:
            result["validation"], result["validation_detail"] = validate_table(
                target_schema, table, before
            )
            if result["validation"] == "PASS":
                result["status"] = "SKIPPED (no new/changed rows)"
                update_log_finish(
                    log_id,
                    "SUCCESS",
                    0,
                    0,
                    0,
                    None,
                    0,
                    0,
                    "PASS",
                    result["validation_detail"],
                )
                update_bronze_log_finish(bronze_id, "SUCCESS", 0, 0)
            else:
                result["status"] = "VALIDATION FAILED"
                result["error"] = result["validation_detail"]
                update_log_finish(
                    log_id,
                    "VALIDATION FAILED",
                    0,
                    0,
                    0,
                    result["validation_detail"],
                    0,
                    0,
                    "FAIL",
                    result["validation_detail"],
                )
                update_bronze_log_finish(
                    bronze_id, "FAILED", 0, 0, result["validation_detail"]
                )
            return result
        n_chunks = max(1, math.ceil(total / batch_size))
        bounds = split_window(watermark_from, watermark_to, n_chunks)
        max_retries = _int_env("ETL_MAX_RETRIES", 3)
        retry_delay = _int_env("ETL_RETRY_DELAY_SECONDS", 10)
        arraysize = _int_env("DATABRICKS_FETCH_SIZE", 5000)
        last_index = len(bounds) - 1
        for index, (start, end) in enumerate(bounds):
            attempt = 0
            while True:
                try:
                    header, rows = fetch_chunk(
                        catalog,
                        schema,
                        table,
                        columns,
                        watermark_column,
                        start,
                        end,
                        first_chunk=(index == 0),
                        last_chunk=(index == last_index),
                        arraysize=arraysize,
                    )
                    extracted = len(rows)
                    inserted = updated = 0
                    if extracted and not dry_run:
                        ensure_target_table(target_schema, table, header)
                        ready = ensure_merge_key(target_schema, table, merge_key)
                        inserted, updated = write_chunk_upsert(
                            header,
                            rows,
                            target_schema,
                            table,
                            merge_key if ready else None,
                        )
                    result["extracted"] += extracted
                    result["chunks"] += 1
                    result["inserted"] += inserted
                    result["updated"] += updated
                    logger.info(
                        "chunk done table=%s chunk=%d rows=%d",
                        table,
                        index,
                        inserted + updated,
                    )
                    break
                except Exception as exc:
                    attempt += 1
                    if not is_retryable_error(exc) or attempt > max_retries:
                        raise
                    logger.warning(
                        "retrying table=%s chunk=%d attempt=%d/%d after %ds",
                        table,
                        index,
                        attempt,
                        max_retries,
                        retry_delay,
                    )
                    time.sleep(retry_delay)
        if dry_run:
            result["status"] = "DRY-RUN"
            update_log_finish(
                log_id, "DRY-RUN", result["extracted"], 0, result["chunks"]
            )
            cancel_bronze_log(bronze_id)
            return result
        result["after"] = count_rows(target_schema, table)
        expected = before + result["inserted"]
        result["validation"], result["validation_detail"] = validate_table(
            target_schema, table, expected
        )
        if result["validation"] == "PASS":
            update_log_finish(
                log_id,
                "SUCCESS",
                result["extracted"],
                result["inserted"] + result["updated"],
                result["chunks"],
                None,
                result["inserted"],
                result["updated"],
                "PASS",
                result["validation_detail"],
            )
            update_bronze_log_finish(
                bronze_id,
                "SUCCESS",
                result["extracted"],
                result["inserted"] + result["updated"],
            )
        else:
            result["status"] = "VALIDATION FAILED"
            result["error"] = result["validation_detail"]
            update_log_finish(
                log_id,
                "VALIDATION FAILED",
                result["extracted"],
                result["inserted"] + result["updated"],
                result["chunks"],
                result["validation_detail"],
                result["inserted"],
                result["updated"],
                "FAIL",
                result["validation_detail"],
            )
            update_bronze_log_finish(
                bronze_id,
                "FAILED",
                result["extracted"],
                result["inserted"] + result["updated"],
                result["validation_detail"],
            )
        return result
    except Exception as exc:
        result["status"] = "FAILED"
        result["error"] = short_error(exc)
        update_log_finish(
            log_id,
            "FAILED",
            result["extracted"],
            result["inserted"] + result["updated"],
            result["chunks"],
            traceback.format_exc(),
            result["inserted"],
            result["updated"],
            "FAIL",
            result["error"],
        )
        update_bronze_log_finish(
            bronze_id,
            "FAILED",
            result["extracted"],
            result["inserted"] + result["updated"],
            result["error"],
        )
        raise
    finally:
        result["seconds"] = time.time() - started


# entrypoint wiring watermarks, chunks and audit logs
def main() -> None:
    started = datetime.now(UTC)
    args = parse_args()
    verify_databricks_config()
    run_id = f"{datetime.now(UTC):%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"
    batch_size = args.batch_size or _int_env("ETL_BATCH_SIZE", 10000)
    catalog = args.databricks_catalog or os.getenv("DATABRICKS_CATALOG", "")
    schema = args.databricks_schema or os.getenv("DATABRICKS_SCHEMA", "default")
    target_schema = args.target_schema or os.getenv("POSTGRES_SCHEMA_SOURCE", "bronze")
    ensure_etl_logs_table()
    if args.tables.strip():
        tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    else:
        tables = discover_tables(catalog, schema)
    if not tables:
        logger.info("no tables to load")
        return
    results: list[dict] = []
    failed: list[str] = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.fields[tbl]}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
    ) as progress:
        task = progress.add_task("extract", total=len(tables), tbl="starting...")
        for table in tables:
            progress.update(task, tbl=table)
            try:
                result = run_table(
                    table,
                    catalog,
                    schema,
                    target_schema,
                    args.watermark_column or None,
                    args.merge_key or None,
                    batch_size,
                    args.full_load,
                    args.job_name,
                    args.dry_run,
                    run_id,
                )
                logger.info("table done name=%s result=%s", table, result)
                results.append(result)
            except Exception as exc:  # noqa: BLE001
                logger.error("table failed name=%s error=%s", table, exc)
                failed.append(table)
                results.append(
                    {
                        "name": table,
                        "mode": "FAILED",
                        "status": "FAILED",
                        "watermark_column": args.watermark_column or None,
                        "total": 0,
                        "extracted": 0,
                        "inserted": 0,
                        "updated": 0,
                        "skipped": 0,
                        "before": 0,
                        "after": 0,
                        "columns": 0,
                        "chunks": 0,
                        "seconds": 0.0,
                        "validation": "N/A",
                        "validation_detail": "",
                        "error": short_error(exc),
                    }
                )
                if not is_retryable_error(exc):
                    logger.error("fatal error, aborting remaining tables")
                    progress.advance(task)
                    break
            progress.advance(task)
    elapsed = (datetime.now(UTC) - started).total_seconds()
    has_issues = render_report(
        "Databricks -> PostgreSQL Extraction Report",
        f"{catalog}.{schema}",
        target_schema,
        run_id,
        args.dry_run,
        results,
        elapsed,
        "Tables",
        args.job_name,
    )
    logger.info("run %s complete issues=%s", run_id, has_issues)
    if failed:
        raise SystemExit(f"failed tables: {', '.join(failed)}")


if __name__ == "__main__":
    main()
