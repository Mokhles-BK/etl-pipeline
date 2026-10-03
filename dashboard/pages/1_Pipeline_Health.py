"""Pipeline health: reads the run log (monitoring.pipeline_runs). Read-only."""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st

os.environ.setdefault("PGSSLMODE", "disable")

from etl.db import connect, fetchall  # noqa: E402

STALE_AFTER_HOURS = 36  # the DAG runs daily; older than this means a missed run

st.set_page_config(page_title="Pipeline health", page_icon="🩺", layout="wide")
st.title("🩺 Pipeline health")


@st.cache_data(ttl=60, show_spinner=False)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = connect()
    try:
        rows = fetchall(conn, sql, params)
    finally:
        conn.close()
    return pd.DataFrame(rows)


try:
    exists = query("SELECT to_regclass('monitoring.pipeline_runs') AS rel").loc[0, "rel"]
except Exception as exc:  # pragma: no cover - environment dependent
    st.error(f"Could not read the run log: {exc}")
    st.stop()

if not exists:
    st.warning("No run log yet. Run the pipeline once (`python -m etl.cli --with-warehouse`).")
    st.stop()

days = st.sidebar.selectbox("Window", [7, 14, 30, 90], index=1, format_func=lambda d: f"Last {d} days")
if st.sidebar.button("Refresh"):
    st.cache_data.clear()
    st.rerun()

runs = query(
    """
    SELECT run_id, stage, source, started_at, finished_at, duration_ms, ok,
           records_fetched, records_loaded, records_rejected, error
    FROM monitoring.pipeline_runs
    WHERE started_at >= now() - make_interval(days => %s)
    ORDER BY started_at DESC
    """,
    (days,),
)
latest = query(
    """
    SELECT DISTINCT ON (stage) stage, ok, finished_at, duration_ms,
           records_fetched, records_loaded, records_rejected, error,
           extract(epoch FROM (now() - finished_at)) / 3600.0 AS age_hours
    FROM monitoring.pipeline_runs
    ORDER BY stage, started_at DESC
    """
)
last_ok = query(
    """
    SELECT stage, max(finished_at) AS finished_at,
           extract(epoch FROM (now() - max(finished_at))) / 3600.0 AS age_hours
    FROM monitoring.pipeline_runs WHERE ok GROUP BY stage
    """
)

if latest.empty:
    st.info("The run log is empty. Run the pipeline once.")
    st.stop()

# --- Current status per stage -------------------------------------------------
st.subheader("Current status")
cols = st.columns(len(latest))
for col, (_, r) in zip(cols, latest.iterrows()):
    with col:
        st.markdown(f"**{r['stage'].title()}**")
        st.metric("Last run", "✅ OK" if r["ok"] else "❌ FAILED", f"{r['age_hours']:.1f} h ago", delta_color="off")
        st.caption(f"{r['duration_ms'] / 1000:.1f}s · fetched {r['records_fetched']:,} · "
                   f"loaded {r['records_loaded']:,} · rejected {r['records_rejected']:,}")
        if not r["ok"] and r["error"]:
            st.error(r["error"])

# --- Freshness ----------------------------------------------------------------
for _, r in last_ok.iterrows():
    if r["age_hours"] > STALE_AFTER_HOURS:
        st.warning(f"No successful **{r['stage']}** for {r['age_hours']:.0f} hours "
                   f"(expected at least every {STALE_AFTER_HOURS} h).")
missing = set(latest["stage"]) - set(last_ok["stage"])
for stage in sorted(missing):
    st.warning(f"The **{stage}** stage has never succeeded.")

# --- Window KPIs --------------------------------------------------------------
st.subheader(f"Last {days} days")
if runs.empty:
    st.info("No runs in this window.")
    st.stop()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Runs", f"{len(runs):,}")
k2.metric("Success rate", f"{runs['ok'].mean() * 100:.0f}%")
k3.metric("Avg duration", f"{runs['duration_ms'].mean() / 1000:.1f}s")
k4.metric("Rejected rows", f"{int(runs['records_rejected'].sum()):,}")

runs["day"] = pd.to_datetime(runs["started_at"]).dt.date
loads = runs[runs["stage"] == "load"]

left, right = st.columns(2)
with left:
    st.markdown("**Records loaded per day (load stage)**")
    if loads.empty:
        st.caption("No load runs in this window.")
    else:
        per_day = loads.groupby("day")[["records_loaded", "records_rejected"]].sum().reset_index()
        st.plotly_chart(
            px.bar(per_day, x="day", y=["records_loaded", "records_rejected"], barmode="group",labels={"value": "rows", "variable": ""}),
            width="stretch",
        )
with right:
    st.markdown("**Run duration over time**")
    st.plotly_chart(
        px.line(runs.sort_values("started_at"), x="started_at", y="duration_ms", color="stage", markers=True),
        width="stretch",
    )

# --- Failures and recent runs -------------------------------------------------
failed = runs[~runs["ok"]]
st.subheader("Failures")
if failed.empty:
    st.success("No failed runs in this window.")
else:
    st.dataframe(failed[["started_at", "stage", "source", "error"]], width="stretch", hide_index=True)

st.subheader("Recent runs")
st.dataframe(
    runs.drop(columns=["day"]).head(50),
    width="stretch",
    hide_index=True,
)
