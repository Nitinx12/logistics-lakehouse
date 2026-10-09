import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _submit import submit
from rich.console import Console
from rich.table import Table

from src.utils.connection import close_connection, get_postgres_connection
from src.utils.tracking import track_stage

JOB_FILE = REPO_ROOT / "src" / "jobs" / "extract" / "mongo" / "mongo_to_postgres.py"
JOB_NAME = "mongo_to_postgres"
JARS = [
    "bson-5.1.4.jar",
    "bson-record-codec-5.1.4.jar",
    "mongo-spark-connector_2.12-10.5.0.jar",
    "mongodb-driver-core-5.1.4.jar",
    "mongodb-driver-sync-5.1.4.jar",
    "postgresql.jar",
]


# prints one clean end summary from the audit rows of the latest run
def print_summary(job_name: str, elapsed: float, code: int) -> None:
    try:
        connection = get_postgres_connection()
    except Exception as exc:  # noqa: BLE001
        print(f"summary unavailable: {exc}")
        return
    try:
        latest = connection.execute(
            "SELECT run_id FROM source.etl_logs "
            "WHERE job_name = %s ORDER BY started_at DESC LIMIT 1",
            (job_name,),
        ).fetchone()
        if not latest:
            print("summary: no audit rows yet")
            return
        rows = connection.execute(
            "SELECT collection_name, status, rows_extracted, rows_loaded, "
            "watermark_to, EXTRACT(EPOCH FROM (finished_at - started_at)) "
            "FROM source.etl_logs WHERE job_name = %s AND run_id = %s "
            "ORDER BY collection_name",
            (job_name, latest[0]),
        ).fetchall()
    finally:
        close_connection(connection)
    total_in = sum(row[2] or 0 for row in rows)
    total_out = sum(row[3] or 0 for row in rows)
    failed = sum(1 for row in rows if row[1] == "FAILED")
    table = Table(
        title=f"mongo extract done: exit={code} elapsed={elapsed:.1f}s failed={failed}"
    )
    table.add_column("collection")
    table.add_column("status")
    table.add_column("extracted", justify="right")
    table.add_column("loaded", justify="right")
    table.add_column("watermark_to")
    table.add_column("secs", justify="right")
    for name, status, extracted, loaded, watermark, seconds in rows:
        table.add_row(
            name,
            status,
            f"{extracted:,}",
            f"{loaded:,}",
            str(watermark),
            f"{seconds or 0:.1f}",
        )
    table.add_row("total", "", f"{total_in:,}", f"{total_out:,}", "", "")
    Console().print(table)


# submits the mongo extract and records it in the run log
def main() -> int:
    started = time.time()
    with track_stage("extract_mongo"):
        code = submit(JOB_FILE, JARS, sys.argv[1:])
        print_summary(JOB_NAME, time.time() - started, code)
        if code != 0:
            raise SystemExit(code)
        return code


if __name__ == "__main__":
    sys.exit(main())
