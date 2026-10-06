# Covers the CI workflow definition without needing GitHub.
from pathlib import Path

WORKFLOW_FILE = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"


def _text() -> str:
    return WORKFLOW_FILE.read_text(encoding="utf-8")


def test_workflow_file_exists() -> None:
    assert WORKFLOW_FILE.is_file()


def test_workflow_runs_on_main_push_and_pull_request() -> None:
    assert "pull_request" in _text()
    assert "[main]" in _text()


def test_workflow_installs_locked_dependencies() -> None:
    assert "uv sync --frozen" in _text()


def test_workflow_runs_repo_lint_and_unit_tests() -> None:
    assert "compileall" in _text()
    assert "pytest tests/unit" in _text()


def test_workflow_applies_migrations_on_postgres() -> None:
    assert "postgres:16-alpine" in _text()
    assert "scripts/run_migrate.py" in _text()
    assert "ops.schema_migrations" in _text()
    assert "information_schema.schemata" in _text()
