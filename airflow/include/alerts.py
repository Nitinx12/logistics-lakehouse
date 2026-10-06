# Shared alerting helpers for lakehouse DAGs (ARCHITECTURE.md §9.4, §13.5).
import os
from datetime import timedelta
from typing import Any


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


def log_task_failure(context: dict[str, Any]) -> None:
    try:
        from src.utils.tracking import track_stage

        task_id = str(context.get("task_instance", "unknown_task"))
        with track_stage(f"airflow_failure_{task_id}"):
            pass
    except Exception:  # noqa: BLE001
        return


def build_default_args(retries: int) -> dict[str, Any]:
    emails = get_alert_emails()
    return {
        "owner": "lakehouse",
        "retries": retries,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "email": emails,
        "email_on_failure": bool(emails),
        "email_on_retry": False,
        "on_failure_callback": log_task_failure,
    }
