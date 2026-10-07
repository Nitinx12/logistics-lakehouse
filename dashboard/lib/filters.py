# Sidebar filters shared by dashboard pages.
import pandas as pd
import streamlit as st


def pick_years(frame: pd.DataFrame, key: str) -> list[int]:
    years = sorted(frame["year_number"].dropna().unique().tolist())
    return st.sidebar.multiselect("Years", years, default=years, key=key)


def apply_years(frame: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    return frame[frame["year_number"].isin(years)]


def pick_options(frame: pd.DataFrame, column: str, label: str, key: str) -> list[str]:
    options = sorted(frame[column].dropna().unique().tolist())
    return st.sidebar.multiselect(label, options, default=options, key=key)


def apply_options(frame: pd.DataFrame, column: str, values: list[str]) -> pd.DataFrame:
    return frame[frame[column].isin(values)]


def search_term(label: str, key: str) -> str:
    return st.sidebar.text_input(label, key=key)


def apply_search(frame: pd.DataFrame, column: str, term: str) -> pd.DataFrame:
    if not term.strip():
        return frame
    return frame[frame[column].str.contains(term.strip(), case=False, na=False)]


def min_slider(label: str, maximum: int, key: str) -> int:
    return st.sidebar.slider(label, 0, int(maximum), 0, key=key)
