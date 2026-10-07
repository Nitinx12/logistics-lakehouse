# Shared alerting helpers for lakehouse DAGs (ARCHITECTURE.md §9.4, §13.5).
import os
from datetime import timedelta
from typing import Any


def sla_deadline() -> timedelta:
    try:
        minutes = float(os.getenv("SLO_BATCH_DURATION_P95_MIN", "30")) * 2
    except ValueError:
        minutes = 60.0
    return timedelta(minutes=minutes)


def get_alert_emails() -> list[str]:
    return [
        address.strip()
        for address in os.getenv("LAKEHOUSE_ALERT_EMAILS", "").split(",")
        if address.strip()
    ]


def digest_enabled() -> bool:
    return os.getenv("ALERT_DIGEST_ENABLED", "false").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def get_runbook_url() -> str:
    return os.getenv("LAKEHOUSE_RUNBOOK_URL", "").strip() or "docs/runbook.md"


def _failure_detail(context: dict[str, Any]) -> dict[str, str]:
    dag_id = str(getattr(context.get("dag", None), "dag_id", ""))
    task_id = str(getattr(context.get("task_instance", None), "task_id", ""))
    run_id = str(context.get("run_id", ""))
    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "runbook": get_runbook_url(),
    }


def sla_miss_callback(context: dict[str, Any]) -> None:
    try:
        from src.utils.tracking import track_stage

        with track_stage("airflow_sla_miss") as metrics:
            metrics["detail"] = _failure_detail(context)
    except Exception:  # noqa: BLE001
        return


def log_task_failure(context: dict[str, Any]) -> None:
    try:
        from src.utils.tracking import track_stage

        task_id = str(
            getattr(context.get("task_instance", None), "task_id", "unknown_task")
        )
        with track_stage(f"airflow_failure_{task_id}") as metrics:
            metrics["detail"] = _failure_detail(context)
    except Exception:  # noqa: BLE001
        return


def build_default_args(retries: int) -> dict[str, Any]:
    emails = get_alert_emails()
    return {
        "owner": "lakehouse",
        "retries": retries,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "sla": sla_deadline(),
        "email": emails,
        "email_on_failure": bool(emails) and not digest_enabled(),
        "email_on_retry": False,
        "on_failure_callback": log_task_failure,
        "sla_miss_callback": sla_miss_callback,
        "append_env": True,
    }
