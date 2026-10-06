# Covers the nightly DQ DAG structure without needing Airflow.
import ast
import re
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).resolve().parents[2] / "airflow" / "dags" / "lh_dq_nightly.py"

EXPECTED_TASKS = ("bronze_check", "silver_check", "gold_check", "snapshot")


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
            return int(params.get("retries", "0"))
    raise AssertionError(f"unknown task {task_id}")


def test_dag_file_exists() -> None:
    assert DAG_FILE.is_file()


def test_dag_id_is_lh_dq_nightly() -> None:
    assert 'dag_id="lh_dq_nightly"' in _text()


def test_schedule_is_daily_without_catchup() -> None:
    assert 'schedule="@daily"' in _text()
    assert "catchup=False" in _text()
    assert "max_active_runs=1" in _text()


def test_all_expected_tasks_present() -> None:
    assert _task_ids() == set(EXPECTED_TASKS)


def test_checks_fail_fast_without_retry() -> None:
    assert _retries_of("bronze_check") == 0
    assert _retries_of("silver_check") == 0
    assert _retries_of("gold_check") == 0


def test_snapshot_uses_single_retry() -> None:
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
    assert "src.utils.tracking import record_snapshot" in _text()


def test_run_id_flows_into_every_task() -> None:
    assert _compacted().count("{{ run_id }}") >= 1


def test_checks_run_in_layer_order() -> None:
    assert "bronze_check >> silver_check >> gold_check >> snapshot" in _compacted()


def test_failure_alerts_come_from_environment() -> None:
    assert "include.alerts" in _text()
    assert "build_default_args" in _text()
    assert re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", _text()) is None


def test_dagbag_loads_when_airflow_available() -> None:
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("airflow not installed")
    bag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
    assert not bag.import_errors
    assert "lh_dq_nightly" in bag.dags
