# Customer explorer; reads analytics marts only.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st

from dashboard.lib import charts, db, filters

st.set_page_config(page_title="Customers", layout="wide")
st.title("Customers")

try:
    customers = db.run_query("revenue_by_customer")
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()

types = filters.pick_options(
    customers, "customer_type", "Customer types", "customers-types"
)
term = filters.search_term("Name contains", "customers-search")
minimum = filters.min_slider(
    "Minimum loads", int(customers["loads"].max()), "customers-loads"
)
filtered = filters.apply_search(
    filters.apply_options(customers, "customer_type", types), "customer_name", term
)
filtered = filtered[filtered["loads"] >= minimum]

charts.kpi_cards(
    [
        ("Customers", f"{len(filtered):,}"),
        ("Revenue", charts.money(filtered["revenue"].sum())),
        ("Loads", f"{filtered['loads'].sum():,.0f}"),
    ]
)
charts.scatter_chart(
    filtered, "loads", "revenue", "Loads versus revenue", color_col="customer_type"
)
charts.bar_chart(filtered, "customer_name", "revenue", "Revenue by customer")
charts.top_table(filtered, "Filtered customers", rows=25)
