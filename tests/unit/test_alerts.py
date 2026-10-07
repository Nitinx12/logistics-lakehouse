# Covers shared DAG alert helpers without needing Airflow.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(REPO_ROOT / "airflow") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "airflow"))

from include.alerts import build_default_args, digest_enabled, get_alert_emails


def test_alert_emails_empty_by_default(monkeypatch) -> None:
    monkeypatch.delenv("LAKEHOUSE_ALERT_EMAILS", raising=False)
    assert get_alert_emails() == []


def test_alert_emails_parses_csv(monkeypatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_ALERT_EMAILS", "a@x.com, b@x.com ")
    assert get_alert_emails() == ["a@x.com", "b@x.com"]


def test_default_args_enable_mail_only_with_recipients(monkeypatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_ALERT_EMAILS", "a@x.com")
    args = build_default_args(retries=1)
    assert args["email"] == ["a@x.com"]
    assert args["email_on_failure"] is True
    assert args["email_on_retry"] is False
    assert args["retries"] == 1
    assert callable(args["on_failure_callback"])
    assert args["append_env"] is True


def test_default_args_disable_mail_without_recipients(monkeypatch) -> None:
    monkeypatch.delenv("LAKEHOUSE_ALERT_EMAILS", raising=False)
    args = build_default_args(retries=0)
    assert args["email"] == []
    assert args["email_on_failure"] is False


def test_digest_defaults_off(monkeypatch) -> None:
    monkeypatch.delenv("ALERT_DIGEST_ENABLED", raising=False)
    assert digest_enabled() is False
    monkeypatch.setenv("ALERT_DIGEST_ENABLED", "true")
    assert digest_enabled() is True
