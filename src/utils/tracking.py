import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from json import dumps
from time import perf_counter

from .connection import close_connection, get_postgres_connection
from .logger import get_logger

logger = get_logger(__name__)


# resolves one run id shared by every task in a DAG run
def resolve_run_id() -> str:
    for candidate in (
        os.getenv("WAREHOUSE_RUN_ID", "").strip(),
        os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "").strip(),
    ):
        if candidate:
            return candidate
    return f"local_{datetime.now(UTC):%Y%m%dT%H%M%S}"


# resolves the task retry attempt without failing on bad input
def resolve_attempt() -> int:
    try:
        return int(os.getenv("AIRFLOW_CTX_TRY_NUMBER", "1"))
    except ValueError:
        return 1


RUN_ID = resolve_run_id()
ATTEMPT = resolve_attempt()

SNAPSHOT_STAGES = ("extract", "staging", "warehouse", "analytics")

UPSERT_SQL = """
INSERT INTO ops.pipeline_run_log (
    run_id,
    stage,
    status,
    attempt,
    started_at,
    duration_s,
    rows_in,
    rows_out,
    rows_rejected,
    detail
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::JSONB)
ON CONFLICT (run_id, stage)
DO UPDATE SET
    status = EXCLUDED.status,
    attempt = EXCLUDED.attempt,
    started_at = EXCLUDED.started_at,
    duration_s = EXCLUDED.duration_s,
    rows_in = EXCLUDED.rows_in,
    rows_out = EXCLUDED.rows_out,
    rows_rejected = EXCLUDED.rows_rejected,
    detail = EXCLUDED.detail
"""


# writes one stage row; monitoring must never break the pipeline
def _write_row(
    stage: str,
    status: str,
    started: datetime,
    duration_s: float,
    metrics: dict,
) -> None:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        conn.execute(
            UPSERT_SQL,
            (
                RUN_ID,
                stage,
                status,
                ATTEMPT,
                started,
                round(duration_s, 1),
                metrics.get("rows_in"),
                metrics.get("rows_out"),
                metrics.get("rows_rejected"),
                dumps(metrics.get("detail", {})),
            ),
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("run log write failed stage=%s error=%s", stage, exc)
    finally:
        close_connection(conn)


# records one stage run; the caller fills the metrics dict inside the block
@contextmanager
def track_stage(stage: str) -> Iterator[dict]:
    metrics: dict = {}
    started = datetime.now(UTC)
    begin = perf_counter()
    status = "SUCCESS"
    try:
        yield metrics
    except BaseException:
        status = "FAILED"
        raise
    finally:
        _write_row(stage, status, started, perf_counter() - begin, metrics)


# aggregates stage result rows into run-log metrics
def stage_metrics(stage: str, rows: list[dict]) -> dict:
    metrics: dict = {
        "detail": {row["name"]: row["status"] for row in rows},
    }
    if stage in ("staging", "warehouse", "analytics"):
        metrics["rows_in"] = sum(row.get("staged", 0) for row in rows)
        metrics["rows_out"] = sum(
            row.get("inserted", 0) + row.get("updated", 0) for row in rows
        )
        metrics["rows_rejected"] = sum(row.get("skipped", 0) for row in rows)
    if stage.endswith("-tests") or stage == "master":
        failed_dq = sum(
            1
            for row in rows
            if row["name"].startswith("dq ") and row["status"] != "SUCCESS"
        )
        failed_gx = sum(
            1
            for row in rows
            if row["name"].startswith("gx ") and row["status"] != "SUCCESS"
        )
        metrics["detail"]["dq_failed"] = failed_dq
        metrics["detail"]["gx_failed"] = failed_gx
    return metrics


# captures per-table row counts for the current run
def record_snapshot(run_id: str | None = None) -> None:
    active_run_id = run_id or RUN_ID
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        conn.execute("CALL ops.record_layer_snapshot(%s)", (active_run_id,))
        logger.info("snapshot recorded run_id=%s", active_run_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("snapshot failed run_id=%s error=%s", active_run_id, exc)
    finally:
        close_connection(conn)
