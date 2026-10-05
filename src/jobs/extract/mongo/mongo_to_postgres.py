import argparse
import math
import os
import sys
import time
import traceback
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg.errors
from dotenv import load_dotenv
from pyspark import StorageLevel
from pyspark import __version__ as PYSPARK_VERSION
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs._report import render_report, short_error
from src.utils.connection import (
    get_mongo_client,
    get_postgres_connection,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

JOB_NAME = "mongo_to_postgres"
STRING_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
INCREMENTAL_COLUMN_CANDIDATES = [
    "updated_at",
    "updated_timestamp",
    "updatedAt",
    "created_at",
    "created_timestamp",
    "createdAt",
]


# reads an int env var with a fallback default
def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


# verifies that PySpark matches the installed Mongo connector
def verify_spark_compatibility() -> None:
    if not PYSPARK_VERSION.startswith("3.5."):
        raise RuntimeError(
            "Mongo Spark connector 10.5.0 requires PySpark 3.5.x; "
            f"found PySpark {PYSPARK_VERSION}. Run `uv sync` to install the pinned dependencies."
        )
    jar_dir = PROJECT_ROOT / "jars"
    if next(jar_dir.glob("mongo-spark-connector_2.12-10.4.0.jar"), None) is not None:
        raise RuntimeError(
            "mongo-spark-connector 10.4.0 is incompatible with Spark 3.5 "
            "(NoSuchMethodError on resolveAndBind); replace it with 10.5.0."
        )
    if next(jar_dir.glob("mongo-spark-connector_2.12-10.5.0.jar"), None) is None:
        raise RuntimeError(
            "mongo-spark-connector_2.12-10.5.0.jar missing from jars/; "
            "download it from Maven Central before running the extract."
        )


# identifies exceptions that cannot succeed on another attempt
def is_retryable_error(error: Exception) -> bool:
    message = str(error).lower()
    fatal_markers = (
        "nosuchmethoderror",
        "sparkcontext was stopped",
        "cannot call methods on a stopped sparkcontext",
        "classnotfoundexception",
        "noclassdeffounderror",
    )
    return not any(marker in message for marker in fatal_markers)


# parses CLI arguments for the mongo to postgres job
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incremental MongoDB to Postgres loader"
    )
    parser.add_argument(
        "--collections",
        default="",
        help="Comma separated Mongo collections, empty means all",
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Ignore watermark and reload everything",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Rows per chunk, 0 means ETL_BATCH_SIZE env",
    )
    parser.add_argument("--mongo-database", default="", help="Override MONGO_DB env")
    parser.add_argument(
        "--target-schema", default="", help="Override POSTGRES_SCHEMA_SOURCE env"
    )
    parser.add_argument(
        "--watermark-column",
        default="",
        help="Force a watermark column, empty means auto-detect per collection",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover, count and plan without writing anything",
    )
    parser.add_argument(
        "--job-name", default=JOB_NAME, help="Job name recorded in source.etl_logs"
    )
    return parser.parse_args()


# builds a spark session with local mongo and postgres jars
def build_spark(app_name: str) -> SparkSession:
    verify_spark_compatibility()
    if SparkSession.getActiveSession() is not None:
        return SparkSession.getActiveSession()  # type: ignore[return-value]
    jar_dir = PROJECT_ROOT / "jars"
    jars = sorted(
        str(p) for p in jar_dir.glob("*.jar") if "databricks" not in p.name.lower()
    )
    builder = SparkSession.builder.appName(app_name)
    master = os.getenv("SPARK_MASTER", "")
    if not master and os.getenv("ENVIRONMENT", "local") == "local":
        master = "local[*]"
    if master:
        builder = builder.master(master)
    if jars:
        builder = builder.config("spark.jars", ",".join(jars))
    builder = (
        builder.config(
            "spark.sql.session.timeZone", os.getenv("BUSINESS_TIMEZONE", "Asia/Kolkata")
        )
        .config(
            "spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")
        )
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
    )
    # connector v10 option names; only set when explicitly requested
    partitioner = os.getenv("MONGO_PARTITIONER", "").strip()
    if partitioner:
        builder = builder.config("spark.mongodb.read.partitioner", partitioner)
    partition_mb = os.getenv("MONGO_PARTITION_MB", "").strip()
    if partition_mb:
        builder = builder.config(
            "spark.mongodb.read.partitioner.options.partition.size", partition_mb
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return session


# returns the mongo read uri with fast-fail timeouts
def mongo_read_uri() -> str:
    base = os.getenv("MONGO_URL", "").strip() or (
        f"mongodb://{os.getenv('MONGO_HOST', 'localhost')}:{os.getenv('MONGO_PORT', '27017')}"
    )
    if "serverSelectionTimeoutMS" not in base:
        opts = "serverSelectionTimeoutMS=5000&connectTimeoutMS=10000"
        if "?" in base:
            return f"{base}&{opts}"
        if "://" in base and base.count("/", 3) == 2:
            return f"{base}/?{opts}"
        return f"{base}?{opts}"
    return base


# returns the postgres jdbc url from environment
def jdbc_url() -> str:
    return f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', '')}"


# creates source.etl_logs when it does not exist
def ensure_etl_logs_table() -> None:
    ddl_path = PROJECT_ROOT / "sql" / "01_source_etl_logs.sql"
    ddl = ddl_path.read_text(encoding="utf-8") if ddl_path.exists() else ""
    statements = [s for s in ddl.split(";") if s.strip()] if ddl else []
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            if statements:
                for statement in statements:
                    cur.execute(statement)
            else:
                try:
                    cur.execute("CREATE SCHEMA IF NOT EXISTS source")
                except psycopg.errors.InsufficientPrivilege:
                    logger.info("schema source exists, continuing without create right")
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS source.etl_logs (id BIGSERIAL PRIMARY KEY, "
                    "run_id TEXT, job_name VARCHAR NOT NULL, collection_name VARCHAR NOT NULL, "
                    "target_schema VARCHAR NOT NULL, target_table VARCHAR NOT NULL, "
                    "mode VARCHAR NOT NULL, watermark_column VARCHAR NOT NULL, "
                    "watermark_from TIMESTAMPTZ, watermark_to TIMESTAMPTZ, "
                    "rows_extracted BIGINT NOT NULL DEFAULT 0, rows_loaded BIGINT NOT NULL DEFAULT 0, "
                    "rows_inserted BIGINT NOT NULL DEFAULT 0, rows_updated BIGINT NOT NULL DEFAULT 0, "
                    "chunks INTEGER NOT NULL DEFAULT 0, status VARCHAR NOT NULL, "
                    "validation_status VARCHAR NOT NULL DEFAULT 'N/A', validation_detail TEXT, "
                    "error_message TEXT, started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                    "finished_at TIMESTAMPTZ)"
                )
    finally:
        conn.close()


# fetches the last successful watermark for a collection
def get_last_watermark(job_name: str, collection: str) -> datetime | None:
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT watermark_to FROM source.etl_logs "
                "WHERE job_name = %s AND collection_name = %s AND status = 'SUCCESS' "
                "ORDER BY watermark_to DESC LIMIT 1",
                (job_name, collection),
            )
            row = cur.fetchone()
            if row is None or row[0] is None:
                return None
            value: datetime = row[0]
            return value if value.tzinfo else value.replace(tzinfo=UTC)
    finally:
        conn.close()


# inserts a STARTED audit row and returns its id
def insert_log_start(
    job_name: str,
    collection: str,
    target_schema: str,
    mode: str,
    watermark_column: str | None,
    watermark_from: datetime | None,
    watermark_to: datetime,
    run_id: str = "",
) -> int:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for _ in range(2):
                try:
                    cur.execute(
                        "INSERT INTO source.etl_logs (run_id, job_name, collection_name, target_schema, "
                        "target_table, mode, watermark_column, watermark_from, watermark_to, status) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'STARTED') RETURNING id",
                        (
                            run_id,
                            job_name,
                            collection,
                            target_schema,
                            collection,
                            mode,
                            watermark_column or "",
                            watermark_from,
                            watermark_to,
                        ),
                    )
                    row = cur.fetchone()
                    return int(row[0]) if row else 0
                except psycopg.errors.UniqueViolation:
                    logger.info(
                        "clearing stale STARTED row job=%s collection=%s",
                        job_name,
                        collection,
                    )
                    cur.execute(
                        "DELETE FROM source.etl_logs WHERE job_name = %s "
                        "AND collection_name = %s AND status = 'STARTED'",
                        (job_name, collection),
                    )
            raise RuntimeError(f"could not start audit row for {collection!r}")
    finally:
        conn.close()


# marks an audit row finished with counters and validation outcome
def update_log_finish(
    log_id: int,
    status: str,
    rows_extracted: int,
    rows_loaded: int,
    chunks: int,
    error_message: str | None = None,
    inserted: int = 0,
    updated: int = 0,
    validation: str = "N/A",
    validation_detail: str | None = None,
) -> None:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE source.etl_logs SET status = %s, rows_extracted = %s, "
                "rows_loaded = %s, chunks = %s, error_message = %s, rows_inserted = %s, "
                "rows_updated = %s, validation_status = %s, validation_detail = %s, "
                "finished_at = NOW() WHERE id = %s",
                (
                    status,
                    rows_extracted,
                    rows_loaded,
                    chunks,
                    error_message,
                    inserted,
                    updated,
                    validation,
                    validation_detail,
                    log_id,
                ),
            )
    finally:
        conn.close()


# inserts a bronze STARTED audit row and returns its id
def insert_bronze_log_start(
    job_name: str,
    collection: str,
    watermark_from: datetime | None,
    watermark_to: datetime,
) -> int:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for _ in range(2):
                try:
                    cur.execute(
                        "INSERT INTO bronze.etl_logs (job_name, target_table, "
                        "watermark_from, watermark_to, status) "
                        "VALUES (%s, %s, %s, %s, 'STARTED') RETURNING id",
                        (job_name, collection, watermark_from, watermark_to),
                    )
                    row = cur.fetchone()
                    return int(row[0]) if row else 0
                except psycopg.errors.UniqueViolation:
                    logger.info(
                        "clearing stale bronze STARTED row job=%s collection=%s",
                        job_name,
                        collection,
                    )
                    cur.execute(
                        "DELETE FROM bronze.etl_logs WHERE job_name = %s "
                        "AND target_table = %s AND status = 'STARTED'",
                        (job_name, collection),
                    )
            raise RuntimeError(f"could not start bronze audit row for {collection!r}")
    finally:
        conn.close()


# marks a bronze audit row finished with counters and outcome
def update_bronze_log_finish(
    log_id: int,
    status: str,
    rows_extracted: int,
    rows_loaded: int,
    error_message: str | None = None,
) -> None:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bronze.etl_logs SET status = %s, rows_extracted = %s, "
                "rows_loaded = %s, error_message = %s, finished_at = NOW() "
                "WHERE id = %s",
                (status, rows_extracted, rows_loaded, error_message, log_id),
            )
    finally:
        conn.close()


# removes a bronze STARTED row when a dry run loads nothing
def cancel_bronze_log(log_id: int) -> None:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bronze.etl_logs WHERE id = %s", (log_id,))
    finally:
        conn.close()



# formats a datetime as mongo extended-json $date
def format_mongo_date(value: datetime) -> str:
    aware = value if value.tzinfo else value.replace(tzinfo=UTC)
    return (
        aware.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


# coerces a stored watermark value to utc datetime
def to_utc(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime.strptime(value, STRING_TS_FORMAT).replace(tzinfo=UTC)


# renders one watermark bound for the match stage
def format_mongo_bound(value: datetime, kind: str) -> str:
    if kind == "string":
        aware = value if value.tzinfo else value.replace(tzinfo=UTC)
        return f'"{aware.astimezone(UTC).strftime(STRING_TS_FORMAT)}"'
    return f'{{"$date": "{format_mongo_date(value)}"}}'


# picks the watermark column: an override wins, else first candidate present
def detect_incremental_column(
    sample_fields: list[str], override: str | None
) -> str | None:
    if override:
        return override if override in sample_fields else None
    for candidate in INCREMENTAL_COLUMN_CANDIDATES:
        if candidate in sample_fields:
            return candidate
    return None


# inspects one collection for its watermark column and value kind
def inspect_collection(
    collection: str, watermark_override: str | None, database: str | None = None
) -> tuple[str | None, list[str], str]:
    client = get_mongo_client()
    try:
        db_name = database or os.getenv("MONGO_DB", "erp_source")
        docs = list(client[db_name][collection].find().limit(50))
        fields: list[str] = []
        for doc in docs:
            for key in doc:
                if key not in fields:
                    fields.append(key)
        if watermark_override and watermark_override not in fields:
            raise ValueError(
                f"watermark column {watermark_override!r} not found in collection {collection!r}"
            )
        column = watermark_override or detect_incremental_column(fields, None)
        if column is None:
            return None, fields, "none"
        value = next((d[column] for d in docs if d.get(column) is not None), None)
        if value is None:
            logger.warning(
                "watermark column %s is empty in sample collection=%s",
                column,
                collection,
            )
            return None, fields, "none"
        if isinstance(value, datetime):
            return column, fields, "date"
        if isinstance(value, str):
            return column, fields, "string"
        raise ValueError(
            f"watermark column {column!r} in {collection!r} must be a date or a "
            f"'YYYY-MM-DD HH:MM:SS' string, got {type(value).__name__}"
        )
    finally:
        client.close()


# builds the aggregation pipeline for one watermark chunk
# chunks are (start, end): the first chunk is open at start (> from), later chunks
# include start (>=); every chunk but the last excludes end (<) so boundary docs
# are not loaded twice
def build_pipeline(
    watermark_column: str | None,
    start: datetime | None,
    end: datetime,
    first_chunk: bool,
    last_chunk: bool = True,
    kind: str = "date",
) -> str:
    if watermark_column is None:
        return "[]"
    parts: list[str] = []
    if start is not None:
        lower_op = "$gt" if first_chunk else "$gte"
        parts.append(f'"{lower_op}": {format_mongo_bound(start, kind)}')
    upper_op = "$lte" if last_chunk else "$lt"
    parts.append(f'"{upper_op}": {format_mongo_bound(end, kind)}')
    return '[{"$match": {"' + watermark_column + '": {' + ", ".join(parts) + "}}}]"


# renders one watermark bound for a pymongo filter
def format_pymongo_bound(value: datetime, kind: str) -> datetime | str:
    if kind == "date":
        return value
    aware = value if value.tzinfo else value.replace(tzinfo=UTC)
    return aware.astimezone(UTC).strftime(STRING_TS_FORMAT)


# builds the pymongo range filter for the watermark window
def watermark_range_filter(
    watermark_column: str | None,
    watermark_from: datetime | None,
    watermark_to: datetime,
    kind: str,
) -> dict:
    if watermark_column is None:
        return {}
    upper = format_pymongo_bound(watermark_to, kind)
    if watermark_from is None:
        return {watermark_column: {"$lte": upper}}
    return {
        watermark_column: {
            "$gt": format_pymongo_bound(watermark_from, kind),
            "$lte": upper,
        }
    }


# counts every document in a collection
def count_collection(collection: str, database: str | None = None) -> int:
    client = get_mongo_client()
    try:
        db_name = database or os.getenv("MONGO_DB", "erp_source")
        return client[db_name][collection].count_documents({})
    finally:
        client.close()


# splits the watermark window into time-bounded chunks using only boundary keys
def get_chunk_boundaries(
    collection: str,
    watermark_column: str | None,
    watermark_from: datetime | None,
    watermark_to: datetime,
    batch_size: int,
    database: str | None = None,
    kind: str = "date",
) -> list[tuple[datetime | None, datetime]]:
    client = get_mongo_client()
    try:
        db_name = database or os.getenv("MONGO_DB", "erp_source")
        coll = client[db_name][collection]
        base_filter = watermark_range_filter(
            watermark_column, watermark_from, watermark_to, kind
        )
        total = coll.count_documents(base_filter)
        if total == 0:
            return []
        n_chunks = max(1, math.ceil(total / batch_size))
        if n_chunks == 1 or watermark_column is None:
            return [(watermark_from, watermark_to)]
        edges: list[datetime] = []
        for i in range(n_chunks):
            docs = (
                coll.find(base_filter, {watermark_column: 1})
                .sort(watermark_column, 1)
                .skip(i * batch_size)
                .limit(1)
            )
            for doc in docs:
                value = doc.get(watermark_column)
                if value is not None:
                    edges.append(to_utc(value))
        if not edges:
            return [(watermark_from, watermark_to)]
        bounds: list[tuple[datetime | None, datetime]] = []
        for i, edge in enumerate(edges):
            start = watermark_from if i == 0 else edge
            end = edges[i + 1] if i + 1 < len(edges) else watermark_to
            bounds.append((start, end))
        return bounds
    finally:
        client.close()


# reads one watermark chunk from mongo through the spark connector
def read_mongo_chunk(
    spark: SparkSession,
    database: str,
    collection: str,
    pipeline: str,
) -> DataFrame:
    return (
        spark.read.format("mongodb")
        .option("spark.mongodb.read.connection.uri", mongo_read_uri())
        .option("spark.mongodb.read.database", database)
        .option("spark.mongodb.read.collection", collection)
        .option("aggregation.pipeline", pipeline)
        .load()
    )


# flattens mongo types into a postgres-ready frame
def normalize_mongo_frame(frame: DataFrame, watermark_column: str | None) -> DataFrame:
    if "_id" in frame.columns:
        id_type = frame.schema["_id"].dataType
        if isinstance(id_type, StructType):
            names = [f.name for f in id_type.fields]
            oid_field = (
                "oid" if "oid" in names else ("$oid" if "$oid" in names else names[0])
            )
            frame = frame.withColumn(
                "id", F.col(f"_id.{oid_field}").cast("string")
            ).drop("_id")
        else:
            frame = frame.withColumn("id", F.col("_id").cast("string")).drop("_id")
    complex_cols = [
        f.name
        for f in frame.schema.fields
        if isinstance(f.dataType, (StructType, ArrayType))
    ]
    for column in complex_cols:
        frame = frame.withColumn(column, F.to_json(F.col(column)))
    if watermark_column and watermark_column in frame.columns:
        stamp = F.col(watermark_column).cast("timestamp")
        if isinstance(frame.schema[watermark_column].dataType, StringType):
            # string values are UTC wall time but cast() reads them in the session tz
            session_tz = frame.sparkSession.conf.get("spark.sql.session.timeZone")
            stamp = F.from_utc_timestamp(stamp, session_tz)
        frame = frame.withColumn(watermark_column, stamp)
    return frame.withColumn("_loaded_at", F.current_timestamp())


# maps a spark type name to a postgres column type
def spark_type_to_postgres(type_name: str) -> str:
    mapping = {
        "string": "TEXT",
        "integer": "BIGINT",
        "long": "BIGINT",
        "short": "BIGINT",
        "byte": "BIGINT",
        "double": "DOUBLE PRECISION",
        "float": "DOUBLE PRECISION",
        "decimal": "NUMERIC",
        "boolean": "BOOLEAN",
        "timestamp": "TIMESTAMPTZ",
        "timestamp_ntz": "TIMESTAMP",
        "date": "DATE",
        "binary": "BYTEA",
    }
    return mapping.get(type_name.lower(), "TEXT")


# creates the target table and evolves it with new columns
def ensure_target_table(
    schema: str, table: str, frame: DataFrame, pk: str | None = "id"
) -> None:
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            except psycopg.errors.InsufficientPrivilege:
                logger.info("schema %s exists, continuing without create right", schema)
            definitions = []
            for field in frame.schema.fields:
                pg_type = spark_type_to_postgres(
                    field.dataType.simpleString().split("<")[0].split("(")[0]
                )
                pk_suffix = " PRIMARY KEY" if field.name == pk else ""
                definitions.append(f'"{field.name}" {pg_type}{pk_suffix}')
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({", ".join(definitions)})'
            )
            for field in frame.schema.fields:
                if field.name == pk:
                    continue
                pg_type = spark_type_to_postgres(
                    field.dataType.simpleString().split("<")[0].split("(")[0]
                )
                cur.execute(
                    f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "{field.name}" {pg_type}'
                )
    finally:
        conn.close()


# checks whether a postgres table exists
def table_exists(schema: str, table: str) -> bool:
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


# counts rows in a postgres table, zero when missing
def count_rows(schema: str, table: str) -> int:
    if not table_exists(schema, table):
        return 0
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        conn.close()


# ensures a unique index on the merge key, False means append-only fallback
def ensure_merge_key(schema: str, table: str, pk: str | None) -> bool:
    if pk is None:
        return False
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid "
                "JOIN pg_namespace n ON n.oid = t.relnamespace "
                "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey) "
                "WHERE n.nspname = %s AND t.relname = %s AND a.attname = %s "
                "AND i.indisunique AND i.indnatts = 1",
                (schema, table, pk),
            )
            if cur.fetchone() is not None:
                return True
            cur.execute(
                f'CREATE UNIQUE INDEX IF NOT EXISTS "{table}_{pk.strip("_")}_uidx" '
                f'ON "{schema}"."{table}" ("{pk}")'
            )
            return True
    except Exception:  # noqa: BLE001
        logger.info("no unique merge key table=%s pk=%s using append-only", table, pk)
        return False
    finally:
        conn.close()


# writes one chunk to postgres, returning exact inserted and updated counts
def write_chunk_upsert(
    frame: DataFrame,
    schema: str,
    table: str,
    user: str,
    password: str,
    url: str,
    jdbc_batchsize: int = 5000,
    pk: str | None = "id",
) -> tuple[int, int]:
    stage = f"{table}__stg"
    # quoted so mixed-case table names resolve to the same relation later
    stage_ref = f'"{schema}"."{stage}"'
    target_ref = f'"{schema}"."{table}"'
    (
        frame.write.format("jdbc")
        .option("url", url)
        .option("dbtable", stage_ref)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", str(jdbc_batchsize))
        .option("stringtype", "unspecified")
        .mode("overwrite")
        .save()
    )
    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        columns = frame.columns
        names = ", ".join(f'"{c}"' for c in columns)
        with conn.cursor() as cur:
            # stage timestamps are UTC wall time (jvm runs with user.timezone=UTC)
            cur.execute("SET TIME ZONE 'UTC'")
            if pk:
                updates = ", ".join(
                    f'"{c}" = EXCLUDED."{c}"' for c in columns if c != pk
                )
                if updates:
                    cur.execute(
                        f"WITH upsert AS (INSERT INTO {target_ref} ({names}) "
                        f"SELECT {names} FROM {stage_ref} "
                        f'ON CONFLICT ("{pk}") DO UPDATE SET {updates} '
                        f"RETURNING (xmax = 0) AS inserted) "
                        f"SELECT count(*) FILTER (WHERE inserted), "
                        f"count(*) FILTER (WHERE NOT inserted) FROM upsert"
                    )
                    row = cur.fetchone()
                    return (int(row[0]), int(row[1])) if row else (0, 0)
                cur.execute(
                    f"INSERT INTO {target_ref} ({names}) "
                    f"SELECT {names} FROM {stage_ref} ON CONFLICT DO NOTHING"
                )
                return max(cur.rowcount, 0), 0
            cur.execute(
                f"INSERT INTO {target_ref} ({names}) SELECT {names} FROM {stage_ref}"
            )
            return max(cur.rowcount, 0), 0
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {stage_ref}")
        except Exception:  # noqa: BLE001
            logger.warning("could not drop stage table %s", stage_ref)
        conn.close()


# discovers loadable mongo collections excluding system ones
def discover_collections(database: str) -> list[str]:
    client = get_mongo_client()
    try:
        return [
            c
            for c in client[database].list_collection_names()
            if not c.startswith("system.")
        ]
    finally:
        client.close()


# validates the row count after load against the expected total
def validate_collection(
    target_schema: str, collection: str, expected_after: int
) -> tuple[str, str]:
    actual = count_rows(target_schema, collection)
    if actual == expected_after:
        return "PASS", f"{actual:,} rows confirmed in Postgres"
    return "FAIL", f"expected {expected_after:,}, found {actual:,} in Postgres"


# runs extract and load for a single collection with chunking
def run_collection(
    spark: SparkSession,
    collection: str,
    database: str,
    target_schema: str,
    watermark_override: str | None,
    batch_size: int,
    full_load: bool,
    job_name: str,
    dry_run: bool,
    run_id: str,
) -> dict:
    started = time.time()
    now = datetime.now(UTC)
    # mongo dates are millisecond precision
    watermark_to = now.replace(microsecond=now.microsecond // 1000 * 1000)
    total = count_collection(collection, database)
    before = count_rows(target_schema, collection)
    watermark_column, _, kind = inspect_collection(
        collection, watermark_override, database
    )
    if watermark_column is None:
        logger.info("no watermark column collection=%s using full reload", collection)
        mode = "full (no watermark)"
        watermark_from = None
    elif full_load:
        mode = "FULL"
        watermark_from = None
    else:
        watermark_from = get_last_watermark(job_name, collection)
        mode = "INCREMENTAL" if watermark_from is not None else "FULL"
    logger.info(
        "watermark kind=%s collection=%s column=%s mode=%s",
        kind,
        collection,
        watermark_column,
        mode,
    )
    log_id = insert_log_start(
        job_name,
        collection,
        target_schema,
        mode,
        watermark_column,
        watermark_from,
        watermark_to,
        run_id,
    )
    bronze_id = insert_bronze_log_start(
        job_name, collection, watermark_from, watermark_to
    )
    result = {
        "name": collection,
        "mode": mode,
        "status": "SUCCESS",
        "watermark_column": watermark_column,
        "total": total,
        "extracted": 0,
        "inserted": 0,
        "updated": 0,
        "skipped": total,
        "before": before,
        "after": before,
        "columns": 0,
        "chunks": 0,
        "seconds": 0.0,
        "validation": "N/A",
        "validation_detail": "",
        "error": None,
    }
    try:
        bounds = get_chunk_boundaries(
            collection,
            watermark_column,
            watermark_from,
            watermark_to,
            batch_size,
            database,
            kind,
        )
        if not bounds:
            result["validation"], result["validation_detail"] = validate_collection(
                target_schema, collection, before
            )
            if result["validation"] == "PASS":
                result["status"] = "SKIPPED (no new/changed rows)"
                update_log_finish(
                    log_id,
                    "SUCCESS",
                    0,
                    0,
                    0,
                    None,
                    0,
                    0,
                    "PASS",
                    result["validation_detail"],
                )
                update_bronze_log_finish(bronze_id, "SUCCESS", 0, 0)
            else:
                result["status"] = "VALIDATION FAILED"
                result["error"] = result["validation_detail"]
                update_log_finish(
                    log_id,
                    "VALIDATION FAILED",
                    0,
                    0,
                    0,
                    result["validation_detail"],
                    0,
                    0,
                    "FAIL",
                    result["validation_detail"],
                )
                update_bronze_log_finish(
                    bronze_id, "FAILED", 0, 0, result["validation_detail"]
                )
            return result
        default_parallelism = max(1, spark.sparkContext.defaultParallelism)
        max_retries = _int_env("ETL_MAX_RETRIES", 3)
        retry_delay = _int_env("ETL_RETRY_DELAY_SECONDS", 10)
        jdbc_batchsize = _int_env("JDBC_BATCHSIZE", 5000)
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "")
        url = jdbc_url()
        last_index = len(bounds) - 1
        for index, (start, end) in enumerate(bounds):
            pipeline = build_pipeline(
                watermark_column,
                start,
                end,
                first_chunk=(index == 0),
                last_chunk=(index == last_index),
                kind=kind,
            )
            attempt = 0
            while True:
                prepared = None
                try:
                    raw = read_mongo_chunk(spark, database, collection, pipeline)
                    partitions = min(
                        default_parallelism, max(1, math.ceil(batch_size / 2000))
                    )
                    # persisted so count() and the jdbc write read mongo once
                    prepared = (
                        normalize_mongo_frame(raw, watermark_column)
                        .repartition(partitions)
                        .persist(StorageLevel.MEMORY_AND_DISK)
                    )
                    if not result["columns"]:
                        result["columns"] = len(prepared.columns)
                    extracted = prepared.count()
                    if extracted == 0:
                        break
                    inserted = updated = 0
                    if not dry_run:
                        ensure_target_table(target_schema, collection, prepared)
                        merge_ready = ensure_merge_key(target_schema, collection, "id")
                        inserted, updated = write_chunk_upsert(
                            prepared,
                            target_schema,
                            collection,
                            user,
                            password,
                            url,
                            jdbc_batchsize,
                            "id" if merge_ready else None,
                        )
                    # counters only move once the chunk fully succeeded (retry-safe)
                    result["extracted"] += extracted
                    result["chunks"] += 1
                    result["inserted"] += inserted
                    result["updated"] += updated
                    logger.info(
                        "chunk done collection=%s chunk=%d rows=%d",
                        collection,
                        index,
                        inserted + updated,
                    )
                    break
                except Exception as exc:
                    attempt += 1
                    if not is_retryable_error(exc) or attempt > max_retries:
                        raise
                    logger.warning(
                        "retrying collection=%s chunk=%d attempt=%d/%d after %ds",
                        collection,
                        index,
                        attempt,
                        max_retries,
                        retry_delay,
                    )
                    time.sleep(retry_delay)
                finally:
                    if prepared is not None:
                        prepared.unpersist()
        result["skipped"] = max(total - result["extracted"], 0)
        if dry_run:
            result["status"] = "DRY-RUN"
            update_log_finish(
                log_id, "DRY-RUN", result["extracted"], 0, result["chunks"]
            )
            cancel_bronze_log(bronze_id)
            return result
        result["after"] = count_rows(target_schema, collection)
        expected = before + result["inserted"]
        result["validation"], result["validation_detail"] = validate_collection(
            target_schema, collection, expected
        )
        if result["validation"] == "PASS":
            update_log_finish(
                log_id,
                "SUCCESS",
                result["extracted"],
                result["inserted"] + result["updated"],
                result["chunks"],
                None,
                result["inserted"],
                result["updated"],
                "PASS",
                result["validation_detail"],
            )
            update_bronze_log_finish(
                bronze_id,
                "SUCCESS",
                result["extracted"],
                result["inserted"] + result["updated"],
            )
        else:
            result["status"] = "VALIDATION FAILED"
            result["error"] = result["validation_detail"]
            update_log_finish(
                log_id,
                "VALIDATION FAILED",
                result["extracted"],
                result["inserted"] + result["updated"],
                result["chunks"],
                result["validation_detail"],
                result["inserted"],
                result["updated"],
                "FAIL",
                result["validation_detail"],
            )
            update_bronze_log_finish(
                bronze_id,
                "FAILED",
                result["extracted"],
                result["inserted"] + result["updated"],
                result["validation_detail"],
            )
        return result
    except Exception as exc:
        result["status"] = "FAILED"
        result["error"] = short_error(exc)
        update_log_finish(
            log_id,
            "FAILED",
            result["extracted"],
            result["inserted"] + result["updated"],
            result["chunks"],
            traceback.format_exc(),
            result["inserted"],
            result["updated"],
            "FAIL",
            result["error"],
        )
        update_bronze_log_finish(
            bronze_id,
            "FAILED",
            result["extracted"],
            result["inserted"] + result["updated"],
            result["error"],
        )
        raise
    finally:
        result["seconds"] = time.time() - started


# entrypoint wiring spark, watermarks, chunks and audit logs
def main() -> None:
    started = datetime.now(UTC)
    args = parse_args()
    run_id = f"{datetime.now(UTC):%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"
    batch_size = args.batch_size or _int_env("ETL_BATCH_SIZE", 10000)
    database = args.mongo_database or os.getenv("MONGO_DB", "erp_source")
    target_schema = args.target_schema or os.getenv("POSTGRES_SCHEMA_SOURCE", "source")
    ensure_etl_logs_table()
    if args.collections.strip():
        collections = [c.strip() for c in args.collections.split(",") if c.strip()]
    else:
        collections = discover_collections(database)
    if not collections:
        logger.info("no collections to load")
        return
    spark = build_spark(args.job_name)
    results: list[dict] = []
    failed: list[str] = []
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.fields[coll]}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
        ) as progress:
            task = progress.add_task(
                "extract", total=len(collections), coll="starting..."
            )
            for collection in collections:
                progress.update(task, coll=collection)
                try:
                    result = run_collection(
                        spark,
                        collection,
                        database,
                        target_schema,
                        args.watermark_column or None,
                        batch_size,
                        args.full_load,
                        args.job_name,
                        args.dry_run,
                        run_id,
                    )
                    logger.info("collection done name=%s result=%s", collection, result)
                    results.append(result)
                except Exception as exc:  # noqa: BLE001
                    logger.error("collection failed name=%s error=%s", collection, exc)
                    failed.append(collection)
                    results.append(
                        {
                            "name": collection,
                            "mode": "FAILED",
                            "status": "FAILED",
                            "watermark_column": args.watermark_column or None,
                            "total": 0,
                            "extracted": 0,
                            "inserted": 0,
                            "updated": 0,
                            "skipped": 0,
                            "before": 0,
                            "after": 0,
                            "columns": 0,
                            "chunks": 0,
                            "seconds": 0.0,
                            "validation": "N/A",
                            "validation_detail": "",
                            "error": short_error(exc),
                        }
                    )
                    if not is_retryable_error(exc):
                        logger.error(
                            "fatal spark error, aborting remaining collections"
                        )
                        progress.advance(task)
                        break
                progress.advance(task)
    finally:
        spark.stop()
    elapsed = (datetime.now(UTC) - started).total_seconds()
    has_issues = render_report(
        "MongoDB -> PostgreSQL Extraction Report",
        database,
        target_schema,
        run_id,
        args.dry_run,
        results,
        elapsed,
        "Collections",
        args.job_name,
    )
    logger.info("run %s complete issues=%s", run_id, has_issues)
    if failed:
        raise SystemExit(f"failed collections: {', '.join(failed)}")


if __name__ == "__main__":
    main()
