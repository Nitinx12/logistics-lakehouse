# Covers the stream replay DAG structure without needing Airflow.
import ast
import re
from pathlib import Path

import pytest

DAG_FILE = (
    Path(__file__).resolve().parents[2] / "airflow" / "dags" / "lh_stream_replay.py"
)


def _text() -> str:
    return DAG_FILE.read_text(encoding="utf-8")


def _task_ids() -> set[str]:
    found: set[str] = set()
    for node in ast.walk(ast.parse(_text())):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.value, ast.Call):
            continue
        func = node.value.func
        name = func.id if isinstance(func, ast.Name) else ""
        if name not in {"BashOperator", "PythonOperator"}:
            continue
        for kw in node.value.keywords:
            if kw.arg == "task_id":
                found.add(ast.unparse(kw.value).strip("'\""))
    return found


def test_dag_file_exists() -> None:
    assert DAG_FILE.is_file()


def test_dag_is_manual_only() -> None:
    assert 'dag_id="lh_stream_replay"' in _text()
    assert "schedule=None" in _text()
    assert "catchup=False" in _text()
    assert "max_active_runs=1" in _text()


def test_dag_takes_date_range_params() -> None:
    assert '"start_date"' in _text()
    assert '"end_date"' in _text()
    assert "Param(" in _text()


def test_all_expected_tasks_present() -> None:
    assert _task_ids() == {"preflight", "replay"}


def test_failure_alerts_come_from_environment() -> None:
    assert "include.alerts" in _text()
    assert re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", _text()) is None


def test_dagbag_loads_when_airflow_available() -> None:
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("airflow not installed")
    bag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
    assert not bag.import_errors
    assert "lh_stream_replay" in bag.dags
