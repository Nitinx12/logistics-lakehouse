# Reruns the full batch sequence under one operator supplied batch id.
import os
import sys
from datetime import UTC, datetime

from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from airflow import DAG

sys.path.insert(0, os.path.join(os.getenv("LAKEHOUSE_REPO", "/app"), "airflow"))

from include.alerts import build_default_args
from include.common import (
    script_command,
    snapshot_command,
    task_env,
)


def backfill_env() -> dict[str, str]:
    return {**task_env(), "WAREHOUSE_RUN_ID": "{{ params.batch_id }}"}


with DAG(
    dag_id="lh_backfill",
    description="Manual parameterized rerun of the batch sequence",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param(default="manual_backfill", type="string"),
        "tables": Param(
            default="",
            type="string",
            description="Comma separated tables, empty means all",
        ),
        "start_date": Param(
            default="",
            type="string",
            description="Backfill window start, empty means no lower bound",
        ),
        "end_date": Param(
            default="",
            type="string",
            description="Backfill window end, empty means no upper bound",
        ),
        "full_load": Param(
            default=False,
            type="boolean",
            description="Ignore watermarks and reload the window",
        ),
    },
    default_args=build_default_args(retries=1),
    tags=["lakehouse", "backfill"],
) as dag:
    check = BashOperator(
        task_id="preflight",
        bash_command=script_command(
            "scripts/run_ops.py preflight --systems postgres mongo"
        ),
        env=task_env(),
    )
    watermarks = BashOperator(
        task_id="read_watermarks",
        bash_command=script_command("scripts/run_ops.py read-watermarks"),
        env=task_env(),
    )
    extract_mongo = BashOperator(
        task_id="extract_mongo",
        bash_command=script_command(
            "scripts/run_mongo_job.py --collections {{ params.tables }}"
            "{% if params.full_load %} --full-load{% endif %}"
        ),
        env=task_env(),
        pool="spark_extract",
        retries=3,
    )
    extract_databricks = BashOperator(
        task_id="extract_databricks",
        bash_command=script_command(
            "scripts/run_databricks_job.py --tables {{ params.tables }}"
            "{% if params.full_load %} --full-load{% endif %}"
        ),
        env=task_env(),
        pool="spark_extract",
        retries=3,
    )
    bronze_gate = BashOperator(
        task_id="bronze_gate",
        bash_command=script_command("scripts/run_gx_bronze.py"),
        env=task_env(),
        pool="dq",
        retries=0,
    )
    silver_mongo = BashOperator(
        task_id="silver_mongo",
        bash_command=script_command(
            "scripts/run_silver_mongo.py --batch-id {{ params.batch_id }}"
        ),
        env=task_env(),
        pool="pg_transform",
    )
    silver_databricks = BashOperator(
        task_id="silver_databricks",
        bash_command=script_command(
            "scripts/run_silver_databricks.py --batch-id {{ params.batch_id }}"
        ),
        env=task_env(),
        pool="pg_transform",
    )
    silver_gate = BashOperator(
        task_id="silver_gate",
        bash_command=script_command("scripts/run_gx_silver.py"),
        env=task_env(),
        pool="dq",
        retries=0,
    )
    gold_load = BashOperator(
        task_id="gold_load",
        bash_command=script_command(
            "scripts/run_gold_all.py --batch-id {{ params.batch_id }}"
        ),
        env=task_env(),
        pool="pg_transform",
    )
    gold_gate = BashOperator(
        task_id="gold_gate",
        bash_command=script_command("scripts/run_gx_gold.py"),
        env=task_env(),
        pool="dq",
        retries=0,
    )
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=script_command("scripts/run_ops.py reconcile"),
        env=task_env(),
    )
    freshness = BashOperator(
        task_id="freshness",
        bash_command=script_command("scripts/run_ops.py freshness"),
        env=task_env(),
    )
    advance = BashOperator(
        task_id="advance_watermarks",
        bash_command=script_command("scripts/run_ops.py advance"),
        env=backfill_env(),
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
