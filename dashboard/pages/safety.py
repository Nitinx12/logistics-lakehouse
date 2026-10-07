# Safety incidents; reads analytics marts only.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st

from dashboard.lib import charts, db, filters

st.set_page_config(page_title="Safety", layout="wide")
st.title("Safety")

try:
    kpis = db.run_query("safety_kpis").iloc[0]
    monthly = db.run_query("safety_monthly")
    by_type = db.run_query("claims_by_type")
    by_place = db.run_query("incidents_by_location")
    drivers = db.run_query("driver_scorecard")
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()

years = filters.pick_years(monthly, "safety-years")
monthly = charts.add_period(filters.apply_years(monthly, years))

charts.kpi_cards(
    [
        ("Incidents", f"{kpis['incidents']:,.0f}"),
        ("Total claims", charts.money(kpis["total_claims"])),
        ("Preventable", charts.percent(kpis["preventable_rate"])),
        ("At fault", charts.percent(kpis["at_fault_rate"])),
        ("Injuries", f"{kpis['injuries']:,.0f}"),
    ]
)
charts.trend_chart(monthly, ["incidents"], "Incidents by month")
charts.trend_chart(monthly, ["at_fault", "preventable"], "Fault trend by month")
charts.bar_chart(by_type, "incident_type", "claims", "Claims by incident type")
charts.bar_chart(by_place, "city", "incidents", "Incidents by location")
charts.top_table(
    drivers.sort_values("claims", ascending=False),
    "Drivers by claim exposure",
)
