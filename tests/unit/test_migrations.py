# Covers migration files without needing a database.
import re
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "sql" / "scripts"

EXPECTED_SCHEMAS = ("bronze", "silver", "gold", "ops", "dq", "analytics")

RUNNER = Path(__file__).resolve().parents[2] / "scripts" / "run_migrate.py"


def _versions() -> list[int]:
    versions = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        match = re.fullmatch(r"(\d+)_[a-z0-9_]+\.sql", path.name)
        assert match is not None, path.name
        versions.append(int(match.group(1)))
    return versions


def test_migrations_directory_has_versioned_files() -> None:
    assert MIGRATIONS_DIR.is_dir()
    assert _versions()


def test_versions_start_at_zero_and_do_not_repeat() -> None:
    versions = _versions()
    assert versions[0] == 0
    assert len(set(versions)) == len(versions)
    assert versions == sorted(versions)


def test_v001_creates_all_layer_schemas() -> None:
    text = (MIGRATIONS_DIR / "00_init_schema.sql").read_text(encoding="utf-8")
    for schema in EXPECTED_SCHEMAS:
        assert f"CREATE SCHEMA IF NOT EXISTS {schema};" in text


def test_migrations_end_with_a_semicolon() -> None:
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        assert path.read_text(encoding="utf-8").strip().endswith(";"), path.name


def test_runner_records_each_script_once() -> None:
    text = RUNNER.read_text(encoding="utf-8")
    assert "ops.schema_migrations" in text
    assert "ON CONFLICT (filename) DO NOTHING" in text
