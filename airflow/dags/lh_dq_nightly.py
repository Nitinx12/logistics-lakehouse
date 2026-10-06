# Revalidates bronze silver gold with the full GX checkpoints.
import os
import sys
from datetime import UTC, datetime

from airflow.operators.bash import BashOperator

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


with DAG(
    dag_id="lh_dq_nightly",
    description="Nightly revalidation of bronze silver gold",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=build_default_args(retries=0),
    tags=["lakehouse", "dq"],
) as dag:
    bronze_check = BashOperator(
        task_id="bronze_check",
        bash_command=script_command("scripts/run_gx_bronze.py"),
        env=task_env(),
    )
    silver_check = BashOperator(
        task_id="silver_check",
        bash_command=script_command("scripts/run_gx_silver.py"),
        env=task_env(),
    )
    gold_check = BashOperator(
        task_id="gold_check",
        bash_command=script_command("scripts/run_gx_gold.py"),
        env=task_env(),
    )
    snapshot = BashOperator(
        task_id="snapshot",
        bash_command=snapshot_command(),
        env=task_env(),
        retries=1,
    )
    bronze_check >> silver_check >> gold_check >> snapshot
