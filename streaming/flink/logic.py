# Pure event-time keyed-state logic for trip status (ARCHITECTURE.md §8.5).
from datetime import datetime
from typing import Any

from streaming.message import parse_iso, utc_now_iso


def delay_minutes(message: dict[str, Any]) -> float | None:
    payload = message.get("payload", {})
    try:
        scheduled = parse_iso(str(payload["scheduled_datetime"]))
        actual = parse_iso(str(payload["actual_datetime"]))
    except (KeyError, ValueError):
        return None
    return (actual - scheduled).total_seconds() / 60.0


def to_status(message: dict[str, Any]) -> dict[str, Any]:
    return {
        "trip_id": message["trip_id"],
        "status": str(message["event_type"]),
        "event_ts": message["event_ts"],
        "delay_minutes": delay_minutes(message),
        "schema_version": 1,
        "source": "trip-status-job",
        "produced_at": utc_now_iso(),
    }


def is_late(event_ts: datetime, watermark: datetime, allowed_lateness_s: float) -> bool:
    return (watermark - event_ts).total_seconds() > allowed_lateness_s


def delay_alert(message: dict[str, Any]) -> dict[str, Any] | None:
    flag = str(message.get("payload", {}).get("on_time_flag", "")).strip().lower()
    if flag in ("true", "1", "yes"):
        return None
    if flag not in ("false", "0", "no"):
        return None
    return {
        "trip_id": message["trip_id"],
        "alert_type": "delay",
        "detail": {
            "event_id": message["event_id"],
            "event_type": message["event_type"],
        },
        "event_ts": message["event_ts"],
        "schema_version": 1,
        "source": "trip-status-job",
        "produced_at": utc_now_iso(),
    }


def late_alert(message: dict[str, Any]) -> dict[str, Any]:
    return {
        "trip_id": message["trip_id"],
        "alert_type": "late_data",
        "detail": {"event_id": message["event_id"]},
        "event_ts": message["event_ts"],
        "schema_version": 1,
        "source": "trip-status-job",
        "produced_at": utc_now_iso(),
    }


def stuck_alerts(
    last_seen: dict[str, datetime], now: datetime, stuck_minutes: float
) -> list[dict[str, Any]]:
    alerts = []
    for trip_id, seen_at in last_seen.items():
        if (now - seen_at).total_seconds() > stuck_minutes * 60:
            alerts.append(
                {
                    "trip_id": trip_id,
                    "alert_type": "stuck_trip",
                    "detail": {"last_seen": seen_at.isoformat()},
                    "event_ts": now.isoformat(),
                    "schema_version": 1,
                    "source": "trip-status-job",
                    "produced_at": utc_now_iso(),
                }
            )
    return alerts


def advance_watermark(current_max: datetime | None, event_ts: datetime) -> datetime:
    if current_max is None or event_ts > current_max:
        return event_ts
    return current_max
