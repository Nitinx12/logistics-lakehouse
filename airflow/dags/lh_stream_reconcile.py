# Compares stream and batch counts hourly (ARCHITECTURE.md §8.6, §9.3).
import os
import sys
from datetime import UTC, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, os.path.join(os.getenv("LAKEHOUSE_REPO", "/app"), "airflow"))

from include.alerts import build_default_args
from include.common import snapshot_command, task_env
from include.ops import check_reconcile


def preflight() -> None:
    from include.ops import check_preflight

    check_preflight("postgres")


def reconcile_task() -> None:
    check_reconcile()


with DAG(
    dag_id="lh_stream_reconcile",
    description="Hourly stream versus batch count check",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=build_default_args(retries=0),
    tags=["lakehouse", "streaming"],
) as dag:
    check = PythonOperator(task_id="preflight", python_callable=preflight)
    reconcile = PythonOperator(task_id="reconcile", python_callable=reconcile_task)
    snapshot = BashOperator(
        task_id="snapshot",
        bash_command=snapshot_command(),
        env=task_env(),
        retries=1,
    )
    check >> reconcile >> snapshot
