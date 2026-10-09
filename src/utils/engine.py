from typing import Any

import pandas as pd
from psycopg import sql as postgres_sql
from pymongo import MongoClient

from .connection import (
    close_connection,
    get_databricks_connection,
    get_mongo_client,
    get_mongo_database,
    get_postgres_connection,
)
from .logger import get_logger

logger = get_logger(__name__)


# returns a live connection for the requested source
def get_engine(source: str) -> Any:
    normalized = source.strip().lower()
    if normalized in ("postgres", "postgresql"):
        return get_postgres_connection()
    if normalized in ("mongo", "mongodb"):
        return get_mongo_client()
    if normalized == "databricks":
        return get_databricks_connection()
    raise ValueError(
        f"unknown source {source!r}, expected postgres, mongo or databricks"
    )


# reads a postgres query into a dataframe
def read_postgres(
    query: str,
    params: tuple[Any, ...] | list[Any] | dict[str, Any] | None = None,
) -> pd.DataFrame:
    # manual fetch: pandas read_sql does not reliably accept psycopg3 connections
    connection = get_postgres_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )
            frame = pd.DataFrame(cursor.fetchall(), columns=columns)
        logger.info("postgres read rows=%d", len(frame))
        return frame
    finally:
        close_connection(connection)


# reads a databricks query into a dataframe
def read_databricks(query: str) -> pd.DataFrame:
    connection = get_databricks_connection()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            try:
                # arrow is fastest but needs the pyarrow package
                frame = cursor.fetchall_arrow().to_pandas()
            except Exception:  # noqa: BLE001
                columns = (
                    [desc[0] for desc in cursor.description]
                    if cursor.description
                    else []
                )
                frame = pd.DataFrame(cursor.fetchall(), columns=columns)
            logger.info("databricks read rows=%d", len(frame))
            return frame
        finally:
            cursor.close()
    finally:
        close_connection(connection)


# reads a mongo collection into a dataframe
def read_mongo(
    collection: str,
    query_filter: dict[str, Any] | None = None,
    limit: int | None = None,
    database: str | None = None,
    client: MongoClient | None = None,
) -> pd.DataFrame:
    owned = client is None
    active = client if client is not None else get_mongo_client()
    try:
        handle = active[database] if database else get_mongo_database(active)
        cursor = handle[collection].find(query_filter or {})
        if limit is not None:
            cursor = cursor.limit(limit)
        frame = pd.DataFrame(list(cursor))
        logger.info("mongo read collection=%s rows=%d", collection, len(frame))
        return frame
    finally:
        if owned:
            close_connection(active)


# maps NaN/NaT/None to None, leaves list/dict style values untouched
def _clean_value(value: Any) -> Any:
    try:
        return None if pd.isna(value) else value
    except (TypeError, ValueError):
        return value


# writes a dataframe into postgres and returns the row count
def write_postgres(
    frame: pd.DataFrame,
    table: str,
    schema: str,
    if_exists: str = "append",
) -> int:
    if frame.empty:
        return 0
    if if_exists not in ("append", "replace"):
        raise ValueError(f"unknown if_exists {if_exists!r}, expected append or replace")
    connection = get_postgres_connection()
    try:
        with connection.cursor() as cursor:
            target = postgres_sql.SQL("{}.{}").format(
                postgres_sql.Identifier(schema),
                postgres_sql.Identifier(table),
            )
            if if_exists == "replace":
                cursor.execute(postgres_sql.SQL("TRUNCATE TABLE {}").format(target))
            names = postgres_sql.SQL(", ").join(
                postgres_sql.Identifier(column) for column in frame.columns
            )
            placeholders = postgres_sql.SQL(", ").join(
                postgres_sql.Placeholder() * len(frame.columns)
            )
            insert = postgres_sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                target, names, placeholders
            )
            rows = [
                tuple(_clean_value(value) for value in row)
                for row in frame.itertuples(index=False, name=None)
            ]
            cursor.executemany(insert, rows)
        connection.commit()
        logger.info(
            "postgres wrote schema=%s table=%s rows=%d", schema, table, len(frame)
        )
        return len(frame)
    except Exception:
        connection.rollback()
        raise
    finally:
        close_connection(connection)
