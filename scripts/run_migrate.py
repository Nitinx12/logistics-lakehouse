# Applies sql/scripts in order once each, tracked in ops.schema_migrations.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from psycopg import Connection

from src.utils.connection import close_connection, get_postgres_connection
from src.utils.logger import get_logger

logger = get_logger(__name__)

SCRIPTS_DIR = REPO_ROOT / "sql" / "scripts"

REGISTRY_SQL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.schema_migrations (
    filename VARCHAR PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def list_scripts() -> list[Path]:
    return sorted(SCRIPTS_DIR.glob("*.sql"))


def ensure_registry(connection: Connection) -> None:
    with connection.cursor() as cur:
        cur.execute(REGISTRY_SQL)
    connection.commit()


def is_applied(connection: Connection, filename: str) -> bool:
    with connection.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM ops.schema_migrations WHERE filename = %s",
            (filename,),
        )
        return cur.fetchone() is not None


def apply_script(connection: Connection, path: Path) -> None:
    sql_text = path.read_text(encoding="utf-8")
    if not sql_text.strip():
        raise ValueError(f"migration empty: {path.name}")
    with connection.cursor() as cur:
        cur.execute(sql_text)


def record_applied(connection: Connection, filename: str) -> None:
    with connection.cursor() as cur:
        cur.execute(
            "INSERT INTO ops.schema_migrations (filename) VALUES (%s)"
            " ON CONFLICT (filename) DO NOTHING",
            (filename,),
        )


def main() -> int:
    scripts = list_scripts()
    if not scripts:
        raise FileNotFoundError(f"no migration scripts in {SCRIPTS_DIR}")
    connection = get_postgres_connection()
    try:
        connection.autocommit = True
        ensure_registry(connection)
        applied_now = 0
        skipped = 0
        for path in scripts:
            if is_applied(connection, path.name):
                skipped += 1
                continue
            try:
                apply_script(connection, path)
            except Exception:
                connection.rollback()
                raise
            record_applied(connection, path.name)
            applied_now += 1
        logger.info(
            "migrate done scripts=%d applied=%d skipped=%d",
            len(scripts),
            applied_now,
            skipped,
        )
        return 0
    finally:
        close_connection(connection)


if __name__ == "__main__":
    sys.exit(main())
