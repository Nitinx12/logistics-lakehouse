# Runs weekly retention and analyze housekeeping (ARCHITECTURE.md §9.3).
import os
import sys
from datetime import UTC, datetime

from airflow.operators.bash import BashOperator

from airflow import DAG

sys.path.insert(0, os.path.join(os.getenv("LAKEHOUSE_REPO", "/app"), "airflow"))

from include.alerts import build_default_args
from include.common import script_command, snapshot_command, task_env

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
    check = BashOperator(
        task_id="preflight",
        bash_command=script_command("scripts/run_ops.py preflight --systems postgres"),
        env=task_env(),
    )
    analyze = BashOperator(
        task_id="analyze",
        bash_command=script_command("scripts/run_ops.py maintenance"),
        env=task_env(),
    )
    snapshot = BashOperator(
        task_id="snapshot",
        bash_command=snapshot_command(),
        env=task_env(),
    )
    check >> analyze >> snapshot
