# Replays delivery events for a date range (ARCHITECTURE.md §8.1, §9.3).
import os
import sys
from datetime import UTC, datetime

from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, os.path.join(os.getenv("LAKEHOUSE_REPO", "/app"), "airflow"))

from include.alerts import build_default_args
from include.common import repo_dir, script_command


def task_env() -> dict[str, str]:
    return {
        "LAKEHOUSE_REPO": repo_dir(),
        "WAREHOUSE_RUN_ID": "{{ run_id }}",
        "REPLAY_START_DATE": "{{ params.start_date }}",
        "REPLAY_END_DATE": "{{ params.end_date }}",
    }


def preflight() -> None:
    from src.utils.connection import get_mongo_url, get_postgres_dsn

    get_postgres_dsn()
    get_mongo_url()


with DAG(
    dag_id="lh_stream_replay",
    description="Manual replay of delivery events for a date range",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": Param(default="", type="string"),
        "end_date": Param(default="", type="string"),
    },
    default_args=build_default_args(retries=1),
    tags=["lakehouse", "streaming"],
) as dag:
    check = PythonOperator(task_id="preflight", python_callable=preflight)
    replay = BashOperator(
        task_id="replay",
        bash_command=script_command("scripts/run_replay.py"),
        env=task_env(),
        retries=3,
    )
    check >> replay
