# Covers the admin DSN used for DDL work without needing a database.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from src.utils.connection import get_postgres_admin_dsn


def test_admin_dsn_targets_superuser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DB", "lakehouse")
    monkeypatch.setenv("POSTGRES_SUPERUSER", "migrator")
    monkeypatch.setenv("POSTGRES_SUPERUSER_PASSWORD", "secret")
    dsn = get_postgres_admin_dsn()
    assert "dbname=lakehouse" in dsn
    assert "user=migrator" in dsn


def test_admin_dsn_defaults_superuser_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DB", "lakehouse")
    monkeypatch.delenv("POSTGRES_SUPERUSER", raising=False)
    monkeypatch.setenv("POSTGRES_SUPERUSER_PASSWORD", "secret")
    assert "user=postgres" in get_postgres_admin_dsn()


def test_admin_dsn_ignores_app_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DB", "lakehouse")
    monkeypatch.setenv("POSTGRES_USER", "etl_writer")
    monkeypatch.setenv("POSTGRES_SUPERUSER_PASSWORD", "secret")
    assert "user=etl_writer" not in get_postgres_admin_dsn()


def test_admin_dsn_requires_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DB", "lakehouse")
    monkeypatch.delenv("POSTGRES_SUPERUSER_PASSWORD", raising=False)
    with pytest.raises(ConnectionError):
        get_postgres_admin_dsn()


def test_admin_dsn_requires_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("POSTGRES_DB", raising=False)
    monkeypatch.setenv("POSTGRES_SUPERUSER_PASSWORD", "secret")
    with pytest.raises(ConnectionError):
        get_postgres_admin_dsn()
