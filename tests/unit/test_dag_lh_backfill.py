# Covers the backfill DAG structure without needing Airflow.
import ast
import re
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).resolve().parents[2] / "airflow" / "dags" / "lh_backfill.py"

EXPECTED_TASKS = (
    "preflight",
    "read_watermarks",
    "extract_mongo",
    "extract_databricks",
    "bronze_gate",
    "silver_mongo",
    "silver_databricks",
    "silver_gate",
    "gold_load",
    "gold_gate",
    "reconcile",
    "freshness",
    "advance_watermarks",
    "snapshot",
)


def _text() -> str:
    return DAG_FILE.read_text(encoding="utf-8")


def _compacted() -> str:
    return " ".join(_text().split())


def _operators() -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    for node in ast.walk(ast.parse(_text())):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.value, ast.Call):
            continue
        func = node.value.func
        name = func.id if isinstance(func, ast.Name) else ""
        if name not in {"BashOperator", "PythonOperator"}:
            continue
        if not isinstance(node.targets[0], ast.Name):
            continue
        keywords = {"operator": name}
        for kw in node.value.keywords:
            keywords[kw.arg or ""] = ast.unparse(kw.value)
        found[node.targets[0].id] = keywords
    return found


def _task_ids() -> set[str]:
    task_ids = set()
    for params in _operators().values():
        task_ids.add(params["task_id"].strip("'\""))
    return task_ids


def _retries_of(task_id: str) -> int:
    for params in _operators().values():
        if params["task_id"].strip("'\"") == task_id:
            return int(params.get("retries", "1"))
    raise AssertionError(f"unknown task {task_id}")


def test_dag_file_exists() -> None:
    assert DAG_FILE.is_file()


def test_dag_id_is_lh_backfill() -> None:
    assert 'dag_id="lh_backfill"' in _text()


def test_dag_is_manual_only() -> None:
    assert "schedule=None" in _text()
    assert "catchup=False" in _text()
    assert "max_active_runs=1" in _text()


def test_dag_takes_batch_id_param() -> None:
    assert '"batch_id"' in _text()
    assert "Param(" in _text()


def test_all_expected_tasks_present() -> None:
    assert _task_ids() == set(EXPECTED_TASKS)


def test_extract_tasks_retry_three_times() -> None:
    assert _retries_of("extract_mongo") == 3
    assert _retries_of("extract_databricks") == 3


def test_gates_fail_fast_without_retry() -> None:
    assert _retries_of("bronze_gate") == 0
    assert _retries_of("silver_gate") == 0
    assert _retries_of("gold_gate") == 0


def test_transform_tasks_use_single_retry() -> None:
    assert _retries_of("silver_mongo") == 1
    assert _retries_of("silver_databricks") == 1
    assert _retries_of("gold_load") == 1
    assert _retries_of("read_watermarks") == 1
    assert _retries_of("reconcile") == 1
    assert _retries_of("freshness") == 1
    assert _retries_of("advance_watermarks") == 1
    assert _retries_of("snapshot") == 1


def test_commands_call_repo_entry_points_only() -> None:
    for name, params in _operators().items():
        if params["operator"] != "BashOperator" or name == "snapshot":
            continue
        command = params["bash_command"]
        assert "scripts/run_" in command, name
        assert "SELECT" not in command, name
        assert "INSERT" not in command, name


def test_snapshot_records_layer_counts() -> None:
    assert "snapshot_command()" in _text()
    common = Path(
        Path(__file__).resolve().parents[2] / "airflow" / "include" / "common.py"
    ).read_text(encoding="utf-8")
    assert "src.utils.tracking import record_snapshot" in common


def test_batch_id_param_flows_into_loads() -> None:
    assert _text().count("--batch-id {{ params.batch_id }}") == 3


def test_tables_param_flows_into_extracts() -> None:
    assert '"tables"' in _text()
    assert "--collections {{ params.tables }}" in _text()
    assert "--tables {{ params.tables }}" in _text()


def test_arch_pools_guard_concurrency() -> None:
    assert _text().count('pool="spark_extract"') == 2
    assert _text().count('pool="pg_transform"') == 3
    assert _text().count('pool="dq"') == 3


def test_layers_run_in_order_behind_gates() -> None:
    compacted = _compacted()
    assert (
        "check >> watermarks >> [extract_mongo, extract_databricks] >> bronze_gate"
        in compacted
    )
    assert (
        "bronze_gate >> [silver_mongo, silver_databricks] >> silver_gate" in compacted
    )
    assert "silver_gate >> gold_load >> gold_gate >> reconcile" in compacted
    assert "reconcile >> freshness >> advance >> snapshot" in compacted


def test_watermarks_advance_last() -> None:
    text = _text()
    assert text.index('task_id="gold_gate"') < text.index(
        'task_id="advance_watermarks"'
    )
    assert text.index('task_id="advance_watermarks"') < text.index('task_id="snapshot"')


def test_failure_alerts_come_from_environment() -> None:
    assert "include.alerts" in _text()
    assert "build_default_args" in _text()
    assert "email_on_failure" in Path(
        Path(__file__).resolve().parents[2] / "airflow" / "include" / "alerts.py"
    ).read_text(encoding="utf-8")
    assert re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", _text()) is None


def test_dagbag_loads_when_airflow_available() -> None:
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("airflow not installed")
    bag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
    assert not bag.import_errors
    assert "lh_backfill" in bag.dags
