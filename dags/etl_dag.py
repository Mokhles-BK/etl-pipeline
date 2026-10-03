"""Airflow DAG: daily incremental ETL load + warehouse transform.

Schedules the USGS Earthquake -> Postgres staging load, then the staging ->
warehouse (star schema) transform. The first run is a full load; every
subsequent run is incremental (only events newer than the per-entity cursor
in staging.load_cursors). The cursor lives in the database so it survives
scheduler restarts and is independent of the scheduler's state.

The warehouse transform runs only if the load task succeeds (Airflow's
default trigger rule) — a failed/partial load should never feed a warehouse
rebuild on stale or incomplete staging data.

The etl package is installed editable in the Airflow venv, so the DAG calls
into the same code the manual CLI uses — one implementation, multiple
entrypoints (CLI, this DAG).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.alerts import notify_failure
from etl.pipeline import run_incremental
from etl.sources.usgs_earthquakes import USGSEarthquakes
from etl.warehouse import run_transform

log = logging.getLogger("etl.dag")

def _alert_on_failure(context) -> None:
    """Send a webhook alert when a task fails after all retries.

    The URL comes from the ALERT_WEBHOOK_URL env var, or else from the Airflow
    Variable `alert_webhook_url`. If neither is set the alert is skipped.
    """
    import os

    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        try:
            from airflow.models import Variable

            url = Variable.get("alert_webhook_url", default_var=None)
        except Exception:  # noqa: BLE001 - alerting must never break the callback
            url = None
    notify_failure(context, webhook_url=url)


DEFAULT_ARGS = {
    "owner": "etl",
    "on_failure_callback": _alert_on_failure,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _set_db_env_from_hook() -> None:
    """Populate DB_* env vars from the Airflow connection, if not already set.

    etl.config validates lazily on first access, so this must run before any
    etl.* module touches the DB. The Airflow PostgresHook connection is the
    single source of truth for credentials in this DAG; everything else
    (USGS_*, BATCH_SIZE, etc.) still comes from the process environment.
    """
    import os

    hook = PostgresHook(postgres_conn_id="etl_db")
    conn_info = hook.connection
    os.environ.setdefault("DB_HOST", conn_info.host or "")
    os.environ.setdefault("DB_PORT", str(conn_info.port or 5432))
    os.environ.setdefault("DB_NAME", conn_info.schema or "")
    os.environ.setdefault("DB_USER", conn_info.login or "")
    os.environ.setdefault("DB_PASSWORD", conn_info.password or "")


def load_cycle(**context):
    """Run one incremental load cycle and push results to XCom."""
    _set_db_env_from_hook()

    # The hook's default cursor returns tuple rows, but the loader reads rows
    # as dicts (row["n"]). Get a fresh connection through our own helper so the
    # cursor factory is RealDictCursor and the connection is closed cleanly.
    from etl.db import connect

    conn = connect()
    try:
        report = run_incremental(USGSEarthquakes(), conn)
    finally:
        conn.close()

    log.info(
        "load cycle: fetched=%d loaded=%s rejected=%d ok=%s",
        report.records_fetched,
        report.records_loaded,
        report.errors_loaded,
        report.ok,
    )

    context["ti"].xcom_push(key="batch_id", value=report.batch_id)
    context["ti"].xcom_push(key="fetched", value=report.records_fetched)
    context["ti"].xcom_push(key="loaded", value=report.records_loaded)
    context["ti"].xcom_push(key="rejected", value=report.errors_loaded)
    context["ti"].xcom_push(key="ok", value=report.ok)

    if not report.ok:
        raise RuntimeError(f"load failed: {report.errors}")

    return report.batch_id


def transform_warehouse_cycle(**context):
    """Run the staging -> warehouse transform and push results to XCom.

    Only runs if load_usgs_earthquakes succeeded (default trigger rule) —
    a failed load should never feed a warehouse rebuild on stale data.
    """
    _set_db_env_from_hook()

    from etl.db import connect

    conn = connect()
    try:
        report = run_transform(conn)
    finally:
        conn.close()

    log.info(
        "warehouse transform: dim_time=%d dim_location=%d dim_magnitude_type=%d fact=%d ok=%s",
        report.dim_time,
        report.dim_location,
        report.dim_magnitude_type,
        report.fact_earthquake_events,
        report.ok,
    )

    context["ti"].xcom_push(key="dim_time", value=report.dim_time)
    context["ti"].xcom_push(key="dim_location", value=report.dim_location)
    context["ti"].xcom_push(key="dim_magnitude_type", value=report.dim_magnitude_type)
    context["ti"].xcom_push(key="fact_earthquake_events", value=report.fact_earthquake_events)
    context["ti"].xcom_push(key="ok", value=report.ok)

    if not report.ok:
        raise RuntimeError(f"warehouse transform failed: {report.errors}")

    return report.fact_earthquake_events


with DAG(
    dag_id="etl_usgs_earthquakes_load",
    description="Daily incremental load from USGS Earthquakes into Postgres staging, then warehouse transform",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 9, 22),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "usgs", "earthquakes", "staging", "warehouse"],
) as dag:

    load = PythonOperator(
        task_id="load_usgs_earthquakes",
        python_callable=load_cycle,
    )

    transform_warehouse = PythonOperator(
        task_id="transform_warehouse",
        python_callable=transform_warehouse_cycle,
    )

    load >> transform_warehouse
