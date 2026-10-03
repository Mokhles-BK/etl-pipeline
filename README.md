# ETL Pipeline

Public API → Postgres staging → warehouse → dashboard.

Milestone 1 (this repo): a manual pipeline that pulls from a public API,
validates records, and loads them idempotently into a normalized Postgres
staging schema. No Airflow/Terraform/dashboard yet — that comes after this
reliably pulls and loads real data.

## Layout

```
etl-pipeline/
├── .env                 # local config (copy from .env.example)
├── pyproject.toml
├── sql/
│   └── staging_schema.sql   # idempotent staging tables
├── scripts/
│   └── etl/
│       ├── config.py        # env config, validated at import
│       ├── db.py            # psycopg2 connection helper
│       ├── models.py        # Pydantic record models
│       ├── pipeline.py      # orchestration + report
│       ├── loader.py        # staging writer (upsert, batch-tagged)
│       ├── cli.py           # python -m etl.cli entrypoint
│       └── sources/
│           ├── __init__.py  # Source ABC
│           └── usgs_earthquakes.py
└── tests/
```

## Data source

**Committed: USGS Earthquakes** (`https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson`) —
no API key needed. The feed returns GeoJSON FeatureCollections with a `limit`
(max 20000) and 1-based `offset` for pagination. The default window is the
last 30 days; at M1.0+ that is ~7,500 events, so a single page covers a full
load. The incremental cursor is the event `time` (epoch ms), not the opaque
event id.

JSONPlaceholder was the previous data source; it was replaced because the
pipeline needed a source with real pagination and a monotonic cursor.

## Setup

Postgres runs locally:

```bash
pip install -e .
python -m etl.cli            # run one load cycle
python -m etl.cli --since 1790109283296  # incremental: only events newer than this (epoch ms)
pytest
```

## Idempotency

Every entity has a natural key (`event_id` for earthquakes) and the loader
uses `ON CONFLICT (event_id) DO UPDATE`, so rerunning never duplicates rows.
Each row is tagged with a UUID `batch_id`, so a run can be inspected or rolled
back at the batch level.

Rows that fail source validation (missing event_id, non-numeric magnitude,
malformed geometry) are routed to `staging.load_errors` instead of crashing
the load. The error table is keyed on `(entity, source_id)` so re-rejecting
the same bad row does not duplicate the error row.

## Dashboard

Reads from the warehouse star schema (read-only):

    pip install -e ".[dashboard]"
    streamlit run dashboard/app.py