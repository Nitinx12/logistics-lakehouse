# Dashboard settings from Streamlit secrets with environment fallback.
import os

import psycopg
import streamlit as st
from dotenv import load_dotenv
from psycopg import Connection

load_dotenv()


def _secret(name: str, default: str) -> str:
    try:
        section = st.secrets.get("postgres", {})
    except (FileNotFoundError, OSError, ValueError, AttributeError):
        return default
    value = section.get(name, "") if hasattr(section, "get") else ""
    return str(value) if value else default


def cache_ttl() -> int:
    try:
        return int(os.getenv("STREAMLIT_CACHE_TTL_SECONDS", "300"))
    except ValueError:
        return 300


def connect() -> Connection:
    return psycopg.connect(
        host=_secret("host", os.getenv("STREAMLIT_PG_HOST", "localhost")),
        port=int(_secret("port", os.getenv("STREAMLIT_PG_PORT", "5432"))),
        dbname=_secret("dbname", os.getenv("POSTGRES_DB", "lakehouse")),
        user=_secret("user", os.getenv("STREAMLIT_PG_USER", "dashboard_ro")),
        password=_secret("password", os.getenv("STREAMLIT_PG_PASSWORD", "")),
    )
