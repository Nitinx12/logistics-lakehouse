# Warehouse reads for the dashboard; analytics marts only.
import pandas as pd
import streamlit as st
from psycopg import Error as PostgresError

from dashboard.lib import config
from dashboard.lib.queries import QUERIES


@st.cache_data(ttl=config.cache_ttl(), show_spinner="Loading warehouse marts...")
def run_query(name: str) -> pd.DataFrame:
    try:
        query = QUERIES[name]
    except KeyError:
        raise ValueError(f"unknown dashboard query: {name}") from None
    try:
        with config.connect() as conn:
            return pd.read_sql(query, conn)
    except PostgresError as exc:
        raise RuntimeError(f"warehouse read failed for {name}: {exc}") from exc
