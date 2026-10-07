# Proves reruns never change state. Skips without a live warehouse.
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest
from psycopg import Connection
from psycopg import Error as PsycopgError

from src.utils.connection import close_connection, get_postgres_connection

HOUSEKEEPING = {
    "silver_batch_id",
    "silver_loaded_at",
    "silver_updated_at",
    "gold_batch_id",
    "gold_loaded_at",
    "gold_updated_at",
}

MASTERS = (
    "silver.load_mongo_all",
    "silver.load_databricks_all",
    "gold.load_gold_all",
)


def snapshot(connection: Connection) -> dict[str, tuple[int, str]]:
    tables = connection.execute(
        "SELECT schemaname, tablename FROM pg_tables "
        "WHERE schemaname IN ('silver', 'gold') "
        "AND tablename <> 'etl_logs' ORDER BY 1, 2"
    ).fetchall()
    state = {}
    for schema, table in tables:
        cols = connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY 1",
            (schema, table),
        ).fetchall()
        business = [c[0] for c in cols if c[0] not in HOUSEKEEPING]
        row = connection.execute(
            "SELECT count(*), md5(string_agg(t::text, '|' ORDER BY t::text)) "
            "FROM (SELECT "
            + ", ".join(business)
            + " FROM "
            + schema
            + "."
            + table
            + ") AS t"
        ).fetchone()
        state[schema + "." + table] = (row[0], row[1])
    return state


def run_masters(connection: Connection, batch_id: str) -> None:
    with connection.cursor() as cur:
        for procedure in MASTERS:
            cur.execute("CALL " + procedure + "(%s)", (batch_id,))


def test_double_run_keeps_business_state() -> None:
    try:
        connection = get_postgres_connection()
    except (ConnectionError, PsycopgError) as exc:
        pytest.skip("no live warehouse: " + str(exc))
    try:
        connection.autocommit = True
        before = snapshot(connection)
        if not before:
            pytest.skip("warehouse has no silver or gold tables")
        stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        run_masters(connection, "smoke_a_" + stamp)
        middle = snapshot(connection)
        run_masters(connection, "smoke_b_" + stamp)
        after = snapshot(connection)
        assert middle == before
        assert after == before
    finally:
        close_connection(connection)
