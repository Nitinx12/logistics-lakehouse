# Small Streamlit chart helpers over warehouse mart frames.
from collections.abc import Sequence
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st


def money(value: Any) -> str:
    if value is None:
        return "-"
    return f"${value:,.0f}"


def percent(value: Any) -> str:
    if value is None:
        return "-"
    return f"{value:.1%}"


def kpi_cards(cards: Sequence[tuple[str, str]]) -> None:
    columns = st.columns(len(cards))
    for column, (label, value) in zip(columns, cards, strict=True):
        column.metric(label, value)


def add_period(frame: pd.DataFrame) -> pd.DataFrame:
    dated = frame.copy()
    dated["period"] = (
        dated["year_number"].astype(str)
        + "-"
        + dated["month_number"].astype(str).str.zfill(2)
    )
    return dated


def trend_chart(frame: pd.DataFrame, y_cols: list[str], title: str) -> None:
    st.subheader(title)
    st.line_chart(frame.set_index("period")[y_cols])


def top_table(frame: pd.DataFrame, title: str, rows: int = 10) -> None:
    st.subheader(title)
    st.dataframe(frame.head(rows), use_container_width=True)


def bar_chart(
    frame: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    horizontal: bool = False,
    rows: int = 15,
) -> None:
    st.subheader(title)
    top = frame.head(rows).set_index(x_col)[[y_col]]
    st.bar_chart(top, horizontal=horizontal)


def scatter_chart(
    frame: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    color_col: str | None = None,
    size_col: str | None = None,
) -> None:
    st.subheader(title)
    st.scatter_chart(frame, x=x_col, y=y_col, color=color_col, size=size_col)


def treemap(frame: pd.DataFrame, path: list[str], values: str, title: str) -> None:
    st.subheader(title)
    figure = px.treemap(frame, path=path, values=values)
    figure.update_layout(margin={"t": 30, "l": 10, "r": 10, "b": 10})
    st.plotly_chart(figure, use_container_width=True)
