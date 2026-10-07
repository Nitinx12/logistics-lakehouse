# Compares stream and batch counts hourly (ARCHITECTURE.md §8.6, §9.3).
import os
import sys
from datetime import UTC, datetime

from airflow.operators.bash import BashOperator

from airflow import DAG

sys.path.insert(0, os.path.join(os.getenv("LAKEHOUSE_REPO", "/app"), "airflow"))

from include.alerts import build_default_args
from include.common import script_command, snapshot_command, task_env

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
    check = BashOperator(
        task_id="preflight",
        bash_command=script_command("scripts/run_ops.py preflight --systems postgres"),
        env=task_env(),
    )
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=script_command("scripts/run_ops.py reconcile"),
        env=task_env(),
    )
    snapshot = BashOperator(
        task_id="snapshot",
        bash_command=snapshot_command(),
        env=task_env(),
        retries=1,
    )
    check >> reconcile >> snapshot
