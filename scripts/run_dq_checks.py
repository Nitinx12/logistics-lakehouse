# Runs every tests/dq check once and fails on the first broken one.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from psycopg import Connection
from psycopg import Error as PsycopgError

from src.utils.connection import close_connection, get_postgres_connection
from src.utils.logger import get_logger

logger = get_logger(__name__)

CHECKS_DIR = REPO_ROOT / "tests" / "dq"


def list_checks() -> list[Path]:
    return sorted(CHECKS_DIR.glob("**/*.sql"))


def run_check(connection: Connection, path: Path) -> None:
    sql_text = path.read_text(encoding="utf-8")
    if not sql_text.strip():
        raise ValueError("check empty: " + path.name)
    with connection.cursor() as cur:
        cur.execute(sql_text)


def main() -> int:
    checks = list_checks()
    if not checks:
        raise FileNotFoundError("no DQ checks in " + str(CHECKS_DIR))
    connection = get_postgres_connection()
    try:
        connection.autocommit = True
        failed = 0
        for path in checks:
            try:
                run_check(connection, path)
            except PsycopgError as exc:
                failed += 1
                logger.error("dq check failed file=%s error=%s", path.name, exc)
                continue
            logger.info("dq check passed file=%s", path.name)
        logger.info("dq done checks=%d failed=%d", len(checks), failed)
        return 1 if failed else 0
    finally:
        close_connection(connection)


if __name__ == "__main__":
    sys.exit(main())
