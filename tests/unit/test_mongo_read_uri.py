# Covers the Spark mongo URI honoring the canonical env names.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from src.jobs.extract.mongo.mongo_to_postgres import mongo_read_uri


def test_read_uri_prefers_canonical_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MongoDB_URI", "mongodb://central:27017/fleet_operations")
    monkeypatch.setenv("MONGO_HOST", "localhost")
    assert mongo_read_uri().startswith("mongodb://central:27017/")


def test_read_uri_falls_back_to_host_parts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MongoDB_URI", raising=False)
    monkeypatch.delenv("MONGO_URL", raising=False)
    monkeypatch.setenv("MONGO_HOST", "mongohost")
    monkeypatch.setenv("MONGO_PORT", "27018")
    assert mongo_read_uri().startswith("mongodb://mongohost:27018/")


def test_read_uri_adds_fast_fail_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MongoDB_URI", raising=False)
    monkeypatch.delenv("MONGO_URL", raising=False)
    uri = mongo_read_uri()
    assert "serverSelectionTimeoutMS=5000" in uri
    assert "connectTimeoutMS=10000" in uri
