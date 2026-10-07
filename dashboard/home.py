# Lakehouse logistics landing page; reads analytics marts only.
import streamlit as st
from lib import charts, db

st.set_page_config(page_title="Lakehouse logistics", layout="wide")
st.title("Lakehouse logistics")
st.caption("Batch verified marts from the gold layer. Pick a page on the left.")

try:
    fleet = db.run_query("fleet_kpis").iloc[0]
    loads = db.run_query("load_kpis").iloc[0]
    safety = db.run_query("safety_kpis").iloc[0]
    revenue = charts.add_period(db.run_query("revenue_trend_monthly"))
    incidents = charts.add_period(db.run_query("incidents_trend_monthly"))
except RuntimeError as exc:
    st.error(str(exc))
    st.stop()

charts.kpi_cards(
    [
        ("Lifetime revenue", charts.money(loads["revenue"])),
        ("Loads", f"{loads['loads']:,.0f}"),
        ("Delivery on-time", charts.percent(loads["delivery_on_time_rate"])),
        ("Trucks", f"{fleet['truck_count']:,.0f}"),
        ("Fleet avg MPG", f"{fleet['fleet_avg_mpg']:.1f}"),
        ("Safety incidents", f"{safety['incidents']:,.0f}"),
    ]
)

st.subheader("Go to")
left, middle, right = st.columns(3)
with left:
    st.page_link("pages/revenue.py", label="Revenue deep dive")
with middle:
    st.page_link("pages/customers.py", label="Customer explorer")
with right:
    st.page_link("pages/fleet.py", label="Fleet and fuel")

charts.trend_chart(revenue, ["revenue"], "Monthly revenue")
charts.trend_chart(incidents, ["incidents"], "Monthly safety incidents")
