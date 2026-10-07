# Warehouse callbacks for lakehouse DAGs (ARCHITECTURE.md §9.2, §9.4).
import os
import shutil
from datetime import UTC, datetime

BRONZE_TABLES = (
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


def _band_pct() -> float:
    try:
        return float(os.getenv("DQ_VOLUME_ANOMALY_BAND_PCT", "30"))
    except ValueError:
        return 30.0


def _stream_drift_pct() -> float:
    try:
        return float(os.getenv("SLO_STREAM_BATCH_DRIFT_PCT", "0.5"))
    except ValueError:
        return 0.5


def _freshness_hours() -> float:
    try:
        return float(os.getenv("SLO_FRESHNESS_HOURS", "24"))
    except ValueError:
        return 24.0


def _retention_days() -> int:
    try:
        return int(os.getenv("BRONZE_RETENTION_DAYS", "14"))
    except ValueError:
        return 14


def check_preflight(*systems: str) -> None:
    from src.utils.connection import get_mongo_client, get_postgres_connection
    from src.utils.tracking import track_stage

    with track_stage("preflight") as metrics:
        wanted = {system.lower() for system in systems} or {"postgres"}
        if "postgres" in wanted:
            from src.utils.connection import close_connection

            connection = get_postgres_connection()
            try:
                with connection.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            finally:
                close_connection(connection)
        if "mongo" in wanted:
            client = get_mongo_client()
            client.close()
        if "databricks" in wanted:
            host = (os.getenv("DATABRICKS_HOST") or "").strip()
            token = (os.getenv("DATABRICKS_TOKEN") or "").strip()
            http_path = (
                os.getenv("DATABRICKS_PATH") or os.getenv("DATABRICKS_HTTP_PATH") or ""
            ).strip()
            if not host or not token or not http_path:
                raise ConnectionError(
                    "DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_PATH must be set"
                )
        free_gb = shutil.disk_usage(os.getenv("LAKEHOUSE_REPO", "/app")).free / (
            1024**3
        )
        metrics["detail"] = {
            "systems": sorted(wanted),
            "disk_free_gb": round(free_gb, 2),
        }
        if free_gb < 1.0:
            raise OSError(f"disk free below 1GB: {free_gb:.2f}GB")


def read_watermarks() -> str:
    from psycopg import sql

    from src.utils.connection import close_connection, get_postgres_connection
    from src.utils.logger import get_logger

    try:
        connection = get_postgres_connection()
    except Exception as exc:  # noqa: BLE001
        get_logger(__name__).warning("watermark read skipped error=%s", exc)
        return ""
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT to_regclass('ops.watermark')")
            if cur.fetchone()[0] is not None:
                cur.execute(
                    sql.SQL("SELECT {} FROM {} ORDER BY {} DESC LIMIT 1").format(
                        sql.Identifier("batch_id"),
                        sql.Identifier("ops", "watermark"),
                        sql.Identifier("advanced_at"),
                    )
                )
                row = cur.fetchone()
                return str(row[0]) if row else ""
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
    from psycopg import sql

    from src.utils.connection import close_connection, get_postgres_connection
    from src.utils.logger import get_logger
    from src.utils.tracking import track_stage

    band = _band_pct()
    stream_band = _stream_drift_pct()
    breaches: list[str] = []
    detail: dict[str, str] = {}
    with track_stage("reconcile") as metrics:
        try:
            connection = get_postgres_connection()
        except Exception as exc:  # noqa: BLE001
            get_logger(__name__).warning("reconcile skipped error=%s", exc)
            return
        try:
            with connection.cursor() as cur:
                for table in BRONZE_TABLES:
                    cur.execute(
                        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                            sql.Identifier("bronze"),
                            sql.Identifier(table),
                        )
                    )
                    bronze = cur.fetchone()[0]
                    cur.execute(
                        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                            sql.Identifier("silver"),
                            sql.Identifier(table),
                        )
                    )
                    silver = cur.fetchone()[0]
                    drift = abs(bronze - silver) / bronze * 100.0 if bronze else 0.0
                    detail[table] = (
                        f"bronze={bronze} silver={silver} drift={drift:.2f}%"
                    )
                    if drift > band:
                        breaches.append(table)
                        get_logger(__name__).warning(
                            "reconcile drift table=%s bronze=%s silver=%s",
                            table,
                            bronze,
                            silver,
                        )
                cur.execute("SELECT to_regclass('bronze.delivery_events_stream')")
                if cur.fetchone()[0] is not None:
                    cur.execute("SELECT COUNT(*) FROM bronze.delivery_events")
                    batch = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM bronze.delivery_events_stream")
                    streamed = cur.fetchone()[0]
                    if batch and streamed:
                        drift = abs(batch - streamed) / batch * 100.0
                        detail["delivery_events_stream"] = (
                            f"batch={batch} stream={streamed} drift={drift:.2f}%"
                        )
                        if drift > stream_band:
                            get_logger(__name__).warning(
                                "stream drift batch=%s stream=%s",
                                batch,
                                streamed,
                            )
                metrics["detail"] = detail
        finally:
            close_connection(connection)
    if breaches:
        raise ValueError(f"reconcile drift above {band}%: {sorted(breaches)}")


def update_freshness() -> None:
    from psycopg import sql

    from src.utils.connection import close_connection, get_postgres_connection
    from src.utils.logger import get_logger
    from src.utils.tracking import track_stage

    threshold = _freshness_hours()
    with track_stage("freshness") as metrics:
        try:
            connection = get_postgres_connection()
        except Exception as exc:  # noqa: BLE001
            get_logger(__name__).warning("freshness skipped error=%s", exc)
            return
        try:
            connection.autocommit = True
            with connection.cursor() as cur:
                cur.execute("SELECT to_regclass('ops.freshness')")
                if cur.fetchone()[0] is None:
                    get_logger(__name__).warning("freshness pending 08 migration")
                    return
                cur.execute("SELECT to_regclass('ops.slo_status')")
                has_slo = cur.fetchone()[0] is not None
                stale: list[str] = []
                for table in BRONZE_TABLES:
                    cur.execute(
                        "SELECT to_regclass(%s)",
                        (f"bronze.{table}",),
                    )
                    if cur.fetchone()[0] is None:
                        continue
                    cur.execute(
                        "SELECT 1 FROM information_schema.columns "
                        "WHERE table_schema = 'bronze' AND table_name = %s "
                        "AND column_name = '_ingested_at'",
                        (table,),
                    )
                    max_ts = None
                    if cur.fetchone() is not None:
                        cur.execute(
                            sql.SQL("SELECT max({}) FROM {}.{}").format(
                                sql.Identifier("_ingested_at"),
                                sql.Identifier("bronze"),
                                sql.Identifier(table),
                            )
                        )
                        max_ts = cur.fetchone()[0]
                    is_fresh = True
                    if max_ts is not None:
                        age_hours = (
                            datetime.now(UTC) - max_ts
                        ).total_seconds() / 3600.0
                        is_fresh = age_hours <= threshold
                        if not is_fresh:
                            stale.append(table)
                    cur.execute(
                        "INSERT INTO ops.freshness "
                        "(layer, table_name, max_loaded_at, is_fresh) "
                        "VALUES ('bronze', %s, %s, %s) "
                        "ON CONFLICT (layer, table_name) DO UPDATE SET "
                        "max_loaded_at = EXCLUDED.max_loaded_at, "
                        "is_fresh = EXCLUDED.is_fresh, "
                        "checked_at = NOW()",
                        (table, max_ts, is_fresh),
                    )
                if has_slo:
                    cur.execute(
                        "INSERT INTO ops.slo_status (sli, measured, target_value, is_met) "
                        "VALUES ('gold_freshness', %s, %s, %s) "
                        "ON CONFLICT (sli) DO UPDATE SET "
                        "measured = EXCLUDED.measured, "
                        "target_value = EXCLUDED.target_value, "
                        "is_met = EXCLUDED.is_met, "
                        "checked_at = NOW()",
                        (len(stale), 0, not stale),
                    )
                metrics["detail"] = {"stale_tables": stale}
                if stale:
                    get_logger(__name__).warning("stale tables=%s", stale)
        except Exception as exc:  # noqa: BLE001
            get_logger(__name__).warning("freshness skipped error=%s", exc)
        finally:
            close_connection(connection)


def advance_watermarks(batch_id: str) -> None:
    from src.utils.connection import close_connection, get_postgres_connection
    from src.utils.tracking import track_stage

    if not batch_id:
        raise ValueError("advance_watermarks needs a batch_id")
    with track_stage("advance_watermarks"):
        connection = get_postgres_connection()
        try:
            connection.autocommit = True
            with connection.cursor() as cur:
                cur.execute("SELECT to_regprocedure('ops.advance_watermark(text)')")
                if cur.fetchone()[0] is None:
                    raise RuntimeError("ops.advance_watermark missing, run migrate")
                cur.execute("CALL ops.advance_watermark(%s)", (batch_id,))
        finally:
            close_connection(connection)


def run_retention() -> None:
    from psycopg import sql

    from src.utils.connection import close_connection, get_postgres_connection
    from src.utils.logger import get_logger
    from src.utils.tracking import track_stage

    days = _retention_days()
    with track_stage("retention") as metrics:
        try:
            connection = get_postgres_connection()
        except Exception as exc:  # noqa: BLE001
            get_logger(__name__).warning("retention skipped error=%s", exc)
            return
        try:
            connection.autocommit = True
            purged: dict[str, int] = {}
            with connection.cursor() as cur:
                for table in BRONZE_TABLES:
                    cur.execute("SELECT to_regclass(%s)", (f"bronze.{table}",))
                    if cur.fetchone()[0] is None:
                        continue
                    cur.execute(
                        "SELECT 1 FROM information_schema.columns "
                        "WHERE table_schema = 'bronze' AND table_name = %s "
                        "AND column_name = '_ingested_at'",
                        (table,),
                    )
                    if cur.fetchone() is None:
                        continue
                    cur.execute(
                        sql.SQL(
                            "DELETE FROM {}.{} "
                            "WHERE _ingested_at < NOW() - make_interval(days => %s)"
                        ).format(
                            sql.Identifier("bronze"),
                            sql.Identifier(table),
                        ),
                        (days,),
                    )
                    purged[table] = cur.rowcount or 0
                cur.execute("SELECT to_regclass('ops.layer_snapshot')")
                if cur.fetchone()[0] is not None:
                    cur.execute(
                        "DELETE FROM ops.layer_snapshot "
                        "WHERE recorded_at < NOW() - make_interval(days => %s)",
                        (days,),
                    )
            metrics["detail"] = purged
        except Exception as exc:  # noqa: BLE001
            get_logger(__name__).warning("retention skipped error=%s", exc)
        finally:
            close_connection(connection)


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
