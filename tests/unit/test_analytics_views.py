# Covers the dashboard to analytics contract without needing a database.
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboard.lib.queries import QUERIES

MIGRATIONS_DIR = REPO_ROOT / "sql" / "scripts"


def _migration_text() -> str:
    parts = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        parts.append(path.read_text(encoding="utf-8"))
    return "\n".join(parts)


def _view_names() -> set[str]:
    return set(
        re.findall(r"CREATE OR REPLACE VIEW analytics\.(\w+)", _migration_text())
    )


def _query_views() -> dict[str, str]:
    found: dict[str, str] = {}
    for name, sql in QUERIES.items():
        match = re.search(r"FROM analytics\.(\w+)", sql)
        assert match is not None, name
        found[name] = match.group(1)
    return found


def test_migrations_define_analytics_views() -> None:
    assert _view_names()


def test_every_query_reads_one_analytics_view() -> None:
    assert len(QUERIES) == len(_query_views())


def test_every_query_view_is_defined() -> None:
    assert set(_query_views().values()) <= _view_names()


def test_every_view_has_a_select_grant() -> None:
    text = _migration_text()
    for view in _view_names():
        assert f"GRANT SELECT ON analytics.{view}" in text, view


def test_dashboard_reads_no_base_table() -> None:
    for name, sql in QUERIES.items():
        assert "FROM gold." not in sql, name
        assert "FROM bronze." not in sql, name
        assert "FROM silver." not in sql, name
