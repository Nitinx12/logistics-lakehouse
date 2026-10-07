# Revenue deep dive; reads analytics marts only.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st

from dashboard.lib import charts, db, filters

st.set_page_config(page_title="Revenue", layout="wide")
st.title("Revenue")

try:
    loads = db.run_query("load_kpis").iloc[0]
    monthly = db.run_query("revenue_trend_monthly")
    customers = db.run_query("revenue_by_customer")
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()

years = filters.pick_years(monthly, "revenue-years")
monthly = charts.add_period(filters.apply_years(monthly, years))
types = filters.pick_options(
    customers, "customer_type", "Customer types", "revenue-types"
)
customers = customers[customers["customer_type"].isin(types)]

charts.kpi_cards(
    [
        ("Loads", f"{loads['loads']:,.0f}"),
        ("Revenue", charts.money(loads["revenue"])),
        ("Avg per load", charts.money(loads["avg_revenue_per_load"])),
        ("Delivery on-time", charts.percent(loads["delivery_on_time_rate"])),
    ]
)
charts.bar_chart(monthly, "period", "revenue", "Revenue by month")
charts.trend_chart(monthly, ["delivery_on_time_rate"], "Delivery on-time trend")
charts.treemap(
    customers, ["customer_type", "customer_name"], "revenue", "Revenue by customer"
)
charts.scatter_chart(
    customers, "loads", "revenue", "Loads versus revenue", color_col="customer_type"
)
charts.top_table(customers, "Customers by revenue")
