# Warehouse callbacks for lakehouse DAGs (ARCHITECTURE.md §9.2, §9.4).
import os
from datetime import UTC, datetime


def _band_pct() -> float:
    try:
        return float(os.getenv("DQ_VOLUME_ANOMALY_BAND_PCT", "30"))
    except ValueError:
        return 30.0


def read_watermarks() -> str:
    from src.utils.connection import close_connection, get_postgres_connection
    from src.utils.logger import get_logger

    try:
        connection = get_postgres_connection()
    except Exception as exc:  # noqa: BLE001
        get_logger(__name__).warning("watermark read skipped error=%s", exc)
        return ""
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT run_id FROM ops.pipeline_run_log "
                "WHERE stage = 'advance_watermarks' AND status = 'SUCCESS' "
                "ORDER BY started_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            return str(row[0]) if row else ""
    except Exception as exc:  # noqa: BLE001
        get_logger(__name__).warning("watermark read skipped error=%s", exc)
        return ""
    finally:
        close_connection(connection)


def check_reconcile() -> None:
    from src.utils.connection import close_connection, get_postgres_connection
    from src.utils.logger import get_logger

    tables = (
        "customers",
        "drivers",
        "facilities",
        "routes",
        "trailers",
        "trucks",
        "loads",
        "trips",
        "fuel_purchases",
        "delivery_events",
        "maintenance_records",
        "safety_incidents",
    )
    band = _band_pct()
    try:
        connection = get_postgres_connection()
    except Exception as exc:  # noqa: BLE001
        get_logger(__name__).warning("reconcile skipped error=%s", exc)
        return
    try:
        with connection.cursor() as cur:
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM bronze.{table}")
                bronze = cur.fetchone()[0]
                cur.execute(f"SELECT COUNT(*) FROM silver.{table}")
                silver = cur.fetchone()[0]
                drift = abs(bronze - silver) / bronze * 100.0 if bronze else 0.0
                if drift > band:
                    get_logger(__name__).warning(
                        "reconcile drift table=%s bronze=%s silver=%s",
                        table,
                        bronze,
                        silver,
                    )
    except Exception as exc:  # noqa: BLE001
        get_logger(__name__).warning("reconcile skipped error=%s", exc)
    finally:
        close_connection(connection)


def update_freshness() -> None:
    from src.utils.connection import close_connection, get_postgres_connection
    from src.utils.logger import get_logger
    from src.utils.tracking import track_stage

    try:
        with track_stage("freshness"):
            connection = get_postgres_connection()
            try:
                with connection.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM ops.layer_snapshot")
            finally:
                close_connection(connection)
    except Exception as exc:  # noqa: BLE001
        get_logger(__name__).warning("freshness skipped error=%s", exc)


def advance_watermarks(batch_id: str) -> None:
    from src.utils.connection import close_connection, get_postgres_connection
    from src.utils.logger import get_logger
    from src.utils.tracking import track_stage

    try:
        with track_stage("advance_watermarks"):
            connection = get_postgres_connection()
            try:
                connection.autocommit = True
                with connection.cursor() as cur:
                    cur.execute("SELECT to_regprocedure('ops.advance_watermark(text)')")
                    if cur.fetchone()[0] is None:
                        get_logger(__name__).warning(
                            "watermark advance pending M1 migration batch=%s",
                            batch_id,
                        )
                        return
                    cur.execute("CALL ops.advance_watermark(%s)", (batch_id,))
            finally:
                close_connection(connection)
    except Exception as exc:  # noqa: BLE001
        get_logger(__name__).warning("watermark advance skipped error=%s", exc)


def run_analyze() -> None:
    from src.utils.connection import close_connection, get_postgres_connection
    from src.utils.logger import get_logger

    try:
        connection = get_postgres_connection()
    except Exception as exc:  # noqa: BLE001
        get_logger(__name__).warning("analyze skipped error=%s", exc)
        return
    try:
        connection.autocommit = True
        with connection.cursor() as cur:
            cur.execute("ANALYZE")
    except Exception as exc:  # noqa: BLE001
        get_logger(__name__).warning("analyze skipped error=%s", exc)
    finally:
        close_connection(connection)


def snapshot_now() -> str:
    return f"snapshot_{datetime.now(UTC):%Y%m%dT%H%M%S}"
