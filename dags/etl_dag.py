"""Airflow DAG: daily incremental ETL load.

Schedules the USGS Earthquake -> Postgres staging load. The first run is a
full load; every subsequent run is incremental (only events newer than the
per-entity cursor in staging.load_cursors). The cursor lives in the database
so it survives scheduler restarts and is independent of the scheduler's state.

The etl package is installed editable in the Airflow venv, so the DAG calls
into the same code the manual CLI uses — one implementation, two entrypoints.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.pipeline import run_incremental
from etl.sources.usgs_earthquakes import USGSEarthquakes

log = logging.getLogger("etl.dag")

DEFAULT_ARGS = {
    "owner": "etl",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def load_cycle(**context):
    """Run one incremental load cycle and push results to XCom."""
    # etl.config reads env vars (DB_*, USGS_*) and validates at first use.
    # The Airflow PostgresHook connection is the single source of truth for
    # the DB credentials here; everything else comes from env.
    hook = PostgresHook(postgres_conn_id="etl_db")
    conn_info = hook.connection
    import os

    os.environ.setdefault("DB_HOST", conn_info.host or "")
    os.environ.setdefault("DB_PORT", str(conn_info.port or 5432))
    os.environ.setdefault("DB_NAME", conn_info.schema or "")
    os.environ.setdefault("DB_USER", conn_info.login or "")
    os.environ.setdefault("DB_PASSWORD", conn_info.password or "")

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


with DAG(
    dag_id="etl_usgs_earthquakes_load",
    description="Daily incremental load from USGS Earthquakes into Postgres staging",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 9, 22),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "usgs", "earthquakes", "staging"],
) as dag:

    load = PythonOperator(
        task_id="load_usgs_earthquakes",
        python_callable=load_cycle,
    )

    load