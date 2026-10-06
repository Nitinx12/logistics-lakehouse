import os

import psycopg
from databricks import sql as databricks_sql
from databricks.sql.client import Connection as DatabricksConnection
from dotenv import load_dotenv
from psycopg import Connection as PostgresConnection
from psycopg.conninfo import make_conninfo
from pymongo import MongoClient
from pymongo.database import Database

from .logger import get_logger

load_dotenv()

logger = get_logger(__name__)


# builds the postgres DSN from environment
def get_postgres_dsn() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "")
    user = os.getenv("POSTGRES_USER", "")
    password = os.getenv("POSTGRES_PASSWORD", "")
    if not dbname or not user:
        raise ConnectionError("POSTGRES_DB and POSTGRES_USER must be set")
    # make_conninfo quotes/escapes values, so passwords with spaces or quotes work
    return make_conninfo(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password or None,
    )


# opens a new postgres connection
def get_postgres_connection() -> PostgresConnection:
    dsn = get_postgres_dsn()
    connection = psycopg.connect(dsn, autocommit=False)
    logger.info(
        "postgres connected host=%s db=%s",
        os.getenv("POSTGRES_HOST", "localhost"),
        os.getenv("POSTGRES_DB", ""),
    )
    return connection


# builds the admin postgres DSN from environment for DDL work
def get_postgres_admin_dsn() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "")
    user = os.getenv("POSTGRES_SUPERUSER", "postgres")
    password = os.getenv("POSTGRES_SUPERUSER_PASSWORD", "")
    if not dbname or not password:
        raise ConnectionError("POSTGRES_DB and POSTGRES_SUPERUSER_PASSWORD must be set")
    return make_conninfo(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


# opens a new admin postgres connection for DDL work
def get_postgres_admin_connection() -> PostgresConnection:
    dsn = get_postgres_admin_dsn()
    connection = psycopg.connect(dsn, autocommit=False)
    logger.info(
        "postgres admin connected host=%s db=%s",
        os.getenv("POSTGRES_HOST", "localhost"),
        os.getenv("POSTGRES_DB", ""),
    )
    return connection


# builds the mongo url from environment (.env names it MongoDB_URI)
def get_mongo_url() -> str:
    for key in ("MongoDB_URI", "MONGO_URL"):
        url = (os.getenv(key) or "").strip()
        if url:
            return url
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    return f"mongodb://{host}:{port}"


# opens a mongo client and fails fast when unreachable
def get_mongo_client() -> MongoClient:
    client: MongoClient = MongoClient(get_mongo_url(), serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception:
        client.close()
        raise
    logger.info("mongo connected db=%s", get_mongo_db_name())
    return client


# resolves the mongo database name from environment
def get_mongo_db_name() -> str:
    return os.getenv("MONGO_DB_NAME") or os.getenv("MONGO_DB") or "fleet_operations"


# returns the configured mongo database
def get_mongo_database(client: MongoClient | None = None) -> Database:
    active = client if client is not None else get_mongo_client()
    return active[get_mongo_db_name()]


# opens a new databricks warehouse connection
def get_databricks_connection() -> DatabricksConnection:
    host = (os.getenv("DATABRICKS_HOST") or "").strip()
    # the connector wants a bare hostname, tolerate a full https:// url
    host = host.replace("https://", "").replace("http://", "").rstrip("/")
    token = (os.getenv("DATABRICKS_TOKEN") or "").strip()
    http_path = (
        os.getenv("DATABRICKS_PATH") or os.getenv("DATABRICKS_HTTP_PATH") or ""
    ).strip()
    catalog = (os.getenv("DATABRICKS_CATALOG") or "").strip()
    schema = (os.getenv("DATABRICKS_SCHEMA") or "").strip()
    if not host or not token or not http_path:
        raise ConnectionError(
            "DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_PATH must be set"
        )
    kwargs = {
        "server_hostname": host,
        "http_path": http_path,
        "access_token": token,
    }
    if catalog:
        kwargs["catalog"] = catalog
    if schema:
        kwargs["schema"] = schema
    connection = databricks_sql.connect(**kwargs)
    logger.info("databricks connected host=%s", host)
    return connection


# closes any open connection or client
def close_connection(connection: object) -> None:
    closer = getattr(connection, "close", None)
    if callable(closer):
        closer()
