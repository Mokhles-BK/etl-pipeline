"""Streamlit dashboard reading from the warehouse star schema.

Run from the project root:
    streamlit run dashboard/app.py

Read-only: it only SELECTs from warehouse.* and never writes. Connection
settings come from the same .env the CLI uses (via etl.config / etl.db).
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st

# Same workaround as tests/conftest.py: the local Postgres has no SSL cert.
os.environ.setdefault("PGSSLMODE", "disable")

from etl.db import connect, fetchall  # noqa: E402

st.set_page_config(page_title="Earthquake Dashboard", page_icon="🌍", layout="wide")

F = "warehouse.fact_earthquake_events"
T = "warehouse.dim_time"
L = "warehouse.dim_location"
M = "warehouse.dim_magnitude_type"


@st.cache_data(ttl=300, show_spinner=False)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Run a read-only query and return a DataFrame (cached for 5 minutes)."""
    conn = connect()
    try:
        rows = fetchall(conn, sql, params)
    finally:
        conn.close()
    return pd.DataFrame(rows)


# --- Guard: warehouse must exist and have data --------------------------------
try:
    bounds = query(f"SELECT min(full_date) AS lo, max(full_date) AS hi FROM {T}")
except Exception as exc:  # pragma: no cover - environment dependent
    st.error(f"Could not read the warehouse: {exc}")
    st.info("Run `python -m etl.cli --with-warehouse` first, and check your .env.")
    st.stop()

if bounds.empty or pd.isna(bounds.loc[0, "lo"]):
    st.warning("The warehouse is empty. Run `python -m etl.cli --with-warehouse` first.")
    st.stop()

lo, hi = bounds.loc[0, "lo"], bounds.loc[0, "hi"]

# --- Sidebar filters ----------------------------------------------------------
st.sidebar.header("Filters")
date_range = st.sidebar.date_input("Date range", value=(lo, hi), min_value=lo, max_value=hi)
if not isinstance(date_range, tuple) or len(date_range) != 2:
    st.sidebar.info("Pick an end date to apply the filter.")
    st.stop()
d_from, d_to = date_range
min_mag = st.sidebar.slider("Minimum magnitude", 0.0, 8.0, 0.0, 0.5)
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()

P = (d_from, d_to, min_mag)
WHERE = "WHERE t.full_date BETWEEN %s AND %s AND f.mag >= %s"
JOIN_T = f"JOIN {T} t ON t.date_key = f.date_key"

st.title("🌍 USGS Earthquakes")
st.caption(f"Source: warehouse star schema · {d_from} → {d_to} · magnitude ≥ {min_mag}")

# --- KPIs ---------------------------------------------------------------------
kpi = query(
    f"""
    SELECT count(*) AS events,
           max(f.mag) AS max_mag,
           avg(f.mag) AS avg_mag,
           count(*) FILTER (WHERE f.tsunami > 0) AS tsunami_flagged,
           max(f.event_time) AS latest_ms
    FROM {F} f {JOIN_T} {WHERE}
    """,
    P,
)
k = kpi.iloc[0]
if int(k["events"]) == 0:
    st.warning("No events match these filters.")
    st.stop()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Events", f"{int(k['events']):,}")
c2.metric("Strongest", f"M {k['max_mag']:.1f}")
c3.metric("Average magnitude", f"{k['avg_mag']:.2f}")
c4.metric("Tsunami-flagged", f"{int(k['tsunami_flagged']):,}")
c5.metric("Latest event (UTC)", pd.to_datetime(int(k["latest_ms"]), unit="ms").strftime("%Y-%m-%d %H:%M"))

# --- Events per day -----------------------------------------------------------
daily = query(
    f"""
    SELECT t.full_date AS day, count(*) AS events, max(f.mag) AS max_mag
    FROM {F} f {JOIN_T} {WHERE}
    GROUP BY t.full_date ORDER BY t.full_date
    """,
    P,
)
left, right = st.columns(2)
with left:
    st.subheader("Events per day")
    st.plotly_chart(px.bar(daily, x="day", y="events", hover_data=["max_mag"]), width="stretch")

# --- Magnitude distribution ---------------------------------------------------
dist = query(
    f"""
    SELECT floor(f.mag)::int AS magnitude, count(*) AS events
    FROM {F} f {JOIN_T} {WHERE}
    GROUP BY 1 ORDER BY 1
    """,
    P,
)
with right:
    st.subheader("Magnitude distribution")
    dist["label"] = dist["magnitude"].astype(str) + "–" + (dist["magnitude"] + 1).astype(str)
    st.plotly_chart(px.bar(dist, x="label", y="events", labels={"label": "Magnitude"}), width="stretch")

# --- Top regions --------------------------------------------------------------
regions = query(
    f"""
    SELECT COALESCE(l.region, 'Unknown') AS region, count(*) AS events, max(f.mag) AS max_mag
    FROM {F} f {JOIN_T}
    LEFT JOIN {L} l ON l.location_key = f.location_key
    {WHERE}
    GROUP BY 1 ORDER BY events DESC LIMIT 15
    """,
    P,
)
left, right = st.columns(2)
with left:
    st.subheader("Top 15 regions")
    fig = px.bar(regions.sort_values("events"), x="events", y="region", orientation="h", hover_data=["max_mag"])
    st.plotly_chart(fig, width="stretch")

# --- Magnitude types ----------------------------------------------------------
types = query(
    f"""
    SELECT COALESCE(m.mag_type, 'unknown') AS mag_type, count(*) AS events
    FROM {F} f {JOIN_T}
    LEFT JOIN {M} m ON m.mag_type_key = f.mag_type_key
    {WHERE}
    GROUP BY 1 ORDER BY events DESC
    """,
    P,
)
with right:
    st.subheader("Magnitude scales used")
    st.plotly_chart(px.bar(types, x="mag_type", y="events"), width="stretch")

# --- Map (location buckets) ---------------------------------------------------
st.subheader("Where events happen")
st.caption("The warehouse stores 1°×1° location buckets, not exact coordinates, so each dot is one bucket.")
geo = query(
    f"""
    SELECT l.lat_bucket + 0.5 AS lat, l.lon_bucket + 0.5 AS lon,
           count(*) AS events, max(f.mag) AS max_mag
    FROM {F} f {JOIN_T}
    JOIN {L} l ON l.location_key = f.location_key
    {WHERE} AND l.lat_bucket IS NOT NULL AND l.lon_bucket IS NOT NULL
    GROUP BY 1, 2
    """,
    P,
)
if geo.empty:
    st.info("No located events for these filters.")
else:
    fig = px.scatter_geo(
        geo, lat="lat", lon="lon", size="events", color="max_mag",
        color_continuous_scale="YlOrRd", projection="natural earth",
        hover_data={"events": True, "max_mag": True, "lat": False, "lon": False},
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=480)
    st.plotly_chart(fig, width="stretch")

# --- Strongest events ---------------------------------------------------------
st.subheader("Strongest events in range")
top = query(
    f"""
    SELECT to_timestamp(f.event_time / 1000.0) AT TIME ZONE 'UTC' AS time_utc,
           f.mag, COALESCE(l.region, 'Unknown') AS region, f.depth, f.event_id
    FROM {F} f {JOIN_T}
    LEFT JOIN {L} l ON l.location_key = f.location_key
    {WHERE}
    ORDER BY f.mag DESC, f.event_time DESC LIMIT 20
    """,
    P,
)
st.dataframe(top, width="stretch", hide_index=True)
