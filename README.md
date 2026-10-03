# ETL Pipeline

![CI](https://github.com/Mokhles-BK/etl-pipeline/actions/workflows/ci.yml/badge.svg)

USGS Earthquake API → Postgres staging → warehouse (star schema) → dashboard,
orchestrated by an Airflow DAG.

The point of the project is real data engineering practice: validated
ingestion, idempotent loads, dimensional modeling, orchestration, and
automated tests in CI.

## Architecture

```
USGS Earthquake API (GeoJSON)
        |
        v
  source.fetch(since=cursor)        validates each record; bad rows are collected, never raised
        |
        v
  staging.earthquakes               upsert on event_id, tagged with a batch_id
  staging.load_errors               rejected rows, keyed on (entity, source_id)
  staging.load_cursors              per-entity incremental cursor (event time, epoch ms)
        |
        v
  warehouse transform               dim_time, dim_location, dim_magnitude_type,
        |                           fact_earthquake_events (upsert on event_id)
        v
  Streamlit dashboard               read-only queries on the warehouse
```

The Airflow DAG `etl_usgs_earthquakes_load` runs daily: `load_usgs_earthquakes`
then `transform_warehouse`. The transform only runs if the load succeeds.

## Layout

```
etl-pipeline/
├── .github/workflows/ci.yml     # runs the test suite against Postgres on every push
├── .env.example                 # copy to .env and fill in
├── pyproject.toml
├── dags/
│   └── etl_dag.py               # Airflow DAG: load >> transform_warehouse
├── dashboard/
│   └── app.py                   # Streamlit dashboard
├── sql/
│   ├── staging_schema.sql       # idempotent staging tables
│   ├── warehouse_schema.sql     # idempotent star schema
│   └── sql_README.md            # design rationale for both schemas
├── scripts/etl/
│   ├── config.py                # env config, validated lazily on first use
│   ├── db.py                    # psycopg2 connection helper
│   ├── models.py                # Pydantic record models
│   ├── loader.py                # staging writer (upsert, batch-tagged)
│   ├── pipeline.py              # run() and run_incremental()
│   ├── warehouse.py             # staging -> star schema transform
│   ├── cli.py                   # python -m etl.cli
│   └── sources/
│       ├── __init__.py          # Source ABC
│       └── usgs_earthquakes.py
└── tests/
```

## Data source

**USGS Earthquakes** (`https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson`),
no API key needed. The feed returns GeoJSON with a `limit` (max 20000) and
1-based `offset` for pagination. The default window is the last 30 days; at
M1.0+ that is ~7,500 events, so a single page covers a full load. The
incremental cursor is the event `time` (epoch ms), not the opaque event id.

## Setup

Requires Postgres. Copy `.env.example` to `.env` and fill in the connection.

```bash
pip install -e ".[dev]"
python -m etl.cli                       # run one load cycle
python -m etl.cli --with-warehouse      # load, then build the warehouse
python -m etl.cli --since 1790109283296 # only events newer than this (epoch ms)
pytest tests/ -v
```

## Dashboard

Reads from the warehouse star schema (read-only):

```bash
pip install -e ".[dashboard]"
streamlit run dashboard/app.py
```

## Airflow

The DAG reads database credentials from an Airflow connection named `etl_db`
and calls the same code as the CLI. To run it once without a scheduler:

```bash
AIRFLOW__CORE__DAGS_FOLDER=<path to>/etl-pipeline/dags airflow dags test etl_usgs_earthquakes_load 2026-09-25
```

## Idempotency

Every entity has a natural key (`event_id` for earthquakes) and the loader
uses `ON CONFLICT (event_id) DO UPDATE`, so rerunning never duplicates rows.
Each row is tagged with a UUID `batch_id`, so a run can be inspected or rolled
back at the batch level. The warehouse transform is idempotent the same way:
dimensions are insert-if-missing and the fact table upserts on `event_id`.

Rows that fail source validation (missing event_id, non-numeric magnitude,
malformed geometry) are routed to `staging.load_errors` instead of crashing
the load. The error table is keyed on `(entity, source_id)` so re-rejecting
the same bad row does not duplicate the error row.

## Tests

`pytest tests/ -v` covers model validation, source pagination, idempotent
loads, cursor handling, the warehouse transform, and a regression test that
the incremental load creates the schema on a fresh database. CI runs the
suite on every push against a Postgres service container.
