# Fleet and fuel; reads analytics marts only.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st

from dashboard.lib import charts, db, filters

st.set_page_config(page_title="Fleet and fuel", layout="wide")
st.title("Fleet and fuel")

try:
    fleet = db.run_query("fleet_kpis").iloc[0]
    trucks = db.run_query("mpg_by_truck")
    fuel = db.run_query("fuel_trend_monthly")
    idle = db.run_query("idle_hours_by_truck")
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()

makes = filters.pick_options(trucks, "make", "Makes", "fleet-makes")
trucks = filters.apply_options(trucks, "make", makes)
years = filters.pick_years(fuel, "fleet-years")
fuel = charts.add_period(filters.apply_years(fuel, years))

charts.kpi_cards(
    [
        ("Trucks", f"{fleet['truck_count']:,.0f}"),
        ("Lifetime miles", f"{fleet['lifetime_miles']:,.0f}"),
        ("Lifetime fuel cost", charts.money(fleet["lifetime_fuel_cost"])),
        ("Fleet avg MPG", f"{fleet['fleet_avg_mpg']:.1f}"),
    ]
)
charts.bar_chart(trucks, "unit_number", "mpg", "MPG by truck", horizontal=True)
charts.bar_chart(
    idle, "unit_number", "idle_hours", "Idle hours by truck", horizontal=True
)
charts.scatter_chart(fuel, "gallons", "total_cost", "Gallons versus cost by month")
charts.trend_chart(fuel, ["avg_price_per_gallon"], "Average fuel price trend")
