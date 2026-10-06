# Verifies gold then publishes the report artifact (ARCHITECTURE.md §9.3).
import os
import sys
from datetime import UTC, datetime

from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, os.path.join(os.getenv("LAKEHOUSE_REPO", "/app"), "airflow"))

from include.alerts import build_default_args  # noqa: E402


def repo_dir() -> str:
    return os.getenv("LAKEHOUSE_REPO", "/app")


def task_env() -> dict[str, str]:
    return {"LAKEHOUSE_REPO": repo_dir(), "WAREHOUSE_RUN_ID": "{{ run_id }}"}


def script_command(script: str) -> str:
    return f'cd "$LAKEHOUSE_REPO" && uv run {script}'


def snapshot_command() -> str:
    return (
        'cd "$LAKEHOUSE_REPO" && uv run python -c '
        '"from src.utils.tracking import record_snapshot; record_snapshot()"'
    )


def preflight() -> None:
    from src.utils.connection import get_postgres_dsn

    get_postgres_dsn()


with DAG(
    dag_id="lh_report_publish",
    description="Gold gate then report publish after batch",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    params={"batch_id": Param(default="manual_backfill", type="string")},
    default_args=build_default_args(retries=1),
    tags=["lakehouse", "report"],
) as dag:
    check = PythonOperator(task_id="preflight", python_callable=preflight)
    verify = BashOperator(
        task_id="verify_gold",
        bash_command=script_command("scripts/run_gx_gold.py"),
        env=task_env(),
        retries=0,
    )
    snapshot = BashOperator(
        task_id="snapshot",
        bash_command=snapshot_command(),
        env=task_env(),
    )
    check >> verify >> snapshot
