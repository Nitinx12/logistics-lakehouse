import os

from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from .logger import get_logger

load_dotenv()

logger = get_logger(__name__)

# python level names -> names spark's setLogLevel accepts
_SPARK_LEVELS = {"WARNING": "WARN", "CRITICAL": "FATAL"}
_VALID_SPARK_LEVELS = {"ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"}


# resolves a spark-safe log level from SPARK_LOG_LEVEL or LOG_LEVEL
def _spark_log_level() -> str:
    raw = (os.getenv("SPARK_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "WARN").upper()
    level = _SPARK_LEVELS.get(raw, raw)
    return level if level in _VALID_SPARK_LEVELS else "WARN"


# returns a configured spark session, reusing the active one when present
def get_spark_session(
    app_name: str = "lakehouse", with_delta: bool = True
) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    builder = SparkSession.builder.appName(app_name)
    master = os.getenv("SPARK_MASTER", "")
    if not master and os.getenv("ENVIRONMENT", "local") == "local":
        master = "local[*]"
    if master:
        builder = builder.master(master)
    # .env names it SPARK_SQL_SHUFFLE_PARTITIONS, older setups used SPARK_SHUFFLE_PARTITIONS
    partitions = (
        os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS")
        or os.getenv("SPARK_SHUFFLE_PARTITIONS")
        or "200"
    )
    builder = builder.config(
        "spark.sql.session.timeZone", os.getenv("BUSINESS_TIMEZONE", "Asia/Kolkata")
    ).config("spark.sql.shuffle.partitions", partitions)
    if with_delta:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(_spark_log_level())
    logger.info("spark connected app=%s master=%s", app_name, master or "existing")
    return session


# stops the given session, or the active one when omitted
def stop_spark_session(session: SparkSession | None = None) -> None:
    target = session if session is not None else SparkSession.getActiveSession()
    if target is not None:
        target.stop()
