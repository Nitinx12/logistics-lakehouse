# Runs weekly retention and analyze housekeeping (ARCHITECTURE.md §9.3).
import os
import sys
from datetime import UTC, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, os.path.join(os.getenv("LAKEHOUSE_REPO", "/app"), "airflow"))

from include.alerts import build_default_args  # noqa: E402
from include.ops import run_analyze  # noqa: E402


def repo_dir() -> str:
    return os.getenv("LAKEHOUSE_REPO", "/app")


def task_env() -> dict[str, str]:
    return {"LAKEHOUSE_REPO": repo_dir(), "WAREHOUSE_RUN_ID": "{{ run_id }}"}


def snapshot_command() -> str:
    return (
        'cd "$LAKEHOUSE_REPO" && uv run python -c '
        '"from src.utils.tracking import record_snapshot; record_snapshot()"'
    )


def preflight() -> None:
    from src.utils.connection import get_postgres_dsn

    get_postgres_dsn()


def analyze_task() -> None:
    run_analyze()


with DAG(
    dag_id="lh_maintenance",
    description="Weekly retention purge and analyze housekeeping",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=build_default_args(retries=1),
    tags=["lakehouse", "maintenance"],
) as dag:
    check = PythonOperator(task_id="preflight", python_callable=preflight)
    analyze = PythonOperator(task_id="analyze", python_callable=analyze_task)
    snapshot = BashOperator(
        task_id="snapshot",
        bash_command=snapshot_command(),
        env=task_env(),
    )
    check >> analyze >> snapshot
