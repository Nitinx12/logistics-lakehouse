# Runs bronze then silver then gold with quality gates (ARCHITECTURE.md §9.2).
import os
import sys
from datetime import UTC, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, os.path.join(os.getenv("LAKEHOUSE_REPO", "/app"), "airflow"))

from include.alerts import build_default_args  # noqa: E402
from include.ops import (  # noqa: E402
    advance_watermarks,
    check_reconcile,
    read_watermarks,
    update_freshness,
)


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
    from src.utils.connection import get_mongo_url, get_postgres_dsn

    get_postgres_dsn()
    get_mongo_url()


def read_watermark_task() -> str:
    return read_watermarks()


def reconcile_task() -> None:
    check_reconcile()


def freshness_task() -> None:
    update_freshness()


def advance_watermark_task() -> None:
    from src.utils.tracking import resolve_run_id

    advance_watermarks(resolve_run_id())


with DAG(
    dag_id="lh_daily_batch",
    description="Daily batch ELT across bronze silver gold",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=build_default_args(retries=1),
    tags=["lakehouse", "batch"],
) as dag:
    check = PythonOperator(task_id="preflight", python_callable=preflight)
    watermarks = PythonOperator(
        task_id="read_watermarks", python_callable=read_watermark_task
    )
    extract_mongo = BashOperator(
        task_id="extract_mongo",
        bash_command=script_command("scripts/run_mongo_job.py"),
        env=task_env(),
        retries=3,
    )
    extract_databricks = BashOperator(
        task_id="extract_databricks",
        bash_command=script_command("scripts/run_databricks_job.py"),
        env=task_env(),
        retries=3,
    )
    bronze_gate = BashOperator(
        task_id="bronze_gate",
        bash_command=script_command("scripts/run_gx_bronze.py"),
        env=task_env(),
        retries=0,
    )
    silver_mongo = BashOperator(
        task_id="silver_mongo",
        bash_command=script_command(
            "scripts/run_silver_mongo.py --batch-id {{ run_id }}"
        ),
        env=task_env(),
    )
    silver_databricks = BashOperator(
        task_id="silver_databricks",
        bash_command=script_command(
            "scripts/run_silver_databricks.py --batch-id {{ run_id }}"
        ),
        env=task_env(),
    )
    silver_gate = BashOperator(
        task_id="silver_gate",
        bash_command=script_command("scripts/run_gx_silver.py"),
        env=task_env(),
        retries=0,
    )
    gold_load = BashOperator(
        task_id="gold_load",
        bash_command=script_command("scripts/run_gold_all.py --batch-id {{ run_id }}"),
        env=task_env(),
    )
    gold_gate = BashOperator(
        task_id="gold_gate",
        bash_command=script_command("scripts/run_gx_gold.py"),
        env=task_env(),
        retries=0,
    )
    reconcile = PythonOperator(task_id="reconcile", python_callable=reconcile_task)
    freshness = PythonOperator(task_id="freshness", python_callable=freshness_task)
    advance = PythonOperator(
        task_id="advance_watermarks", python_callable=advance_watermark_task
    )
    snapshot = BashOperator(
        task_id="snapshot",
        bash_command=snapshot_command(),
        env=task_env(),
    )
    check >> watermarks >> [extract_mongo, extract_databricks] >> bronze_gate
    bronze_gate >> [silver_mongo, silver_databricks] >> silver_gate
    silver_gate >> gold_load >> gold_gate >> reconcile
    reconcile >> freshness >> advance >> snapshot
