# Maintenance and upkeep; reads analytics marts only.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st

from dashboard.lib import charts, db, filters

st.set_page_config(page_title="Maintenance", layout="wide")
st.title("Maintenance")

try:
    kpis = db.run_query("safety_kpis").iloc[0]
    monthly = db.run_query("maintenance_monthly")
    by_type = db.run_query("maintenance_by_type")
    by_truck = db.run_query("maintenance_by_truck")
    scorecard = db.run_query("truck_scorecard")
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()

years = filters.pick_years(monthly, "upkeep-years")
monthly = charts.add_period(filters.apply_years(monthly, years))

charts.kpi_cards(
    [
        ("Lifetime upkeep cost", charts.money(kpis["maintenance_cost"])),
        ("Downtime hours", f"{kpis['downtime_hours']:,.0f}"),
        ("Records in view", f"{monthly['records'].sum():,.0f}"),
    ]
)
charts.trend_chart(monthly, ["total_cost"], "Upkeep cost by month")
charts.trend_chart(monthly, ["downtime_hours"], "Downtime by month")
charts.bar_chart(by_type, "maintenance_type", "total_cost", "Cost by maintenance type")
charts.bar_chart(by_truck, "unit_number", "cost", "Upkeep cost by truck")
charts.top_table(
    scorecard.sort_values("downtime_hours", ascending=False),
    "Trucks by downtime",
)
