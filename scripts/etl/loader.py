"""Staging loader.

Writes normalized records into Postgres staging tables. Idempotent by design:
every entity has a natural key (the source id) and the load uses
ON CONFLICT (id) DO UPDATE, so a rerun never duplicates rows. A UUID batch_id
tags every row so a run can be inspected or rolled back at the batch level.

Records that fail source validation are routed to staging.load_errors instead
of raising, so one bad row can't crash a whole load cycle.
"""

from __future__ import annotations

import uuid
from typing import Iterable

from etl.db import executemany, execute, fetchall, fetchone

# ---------------------------------------------------------------------------
# earthquakes  (USGS GeoJSON feature)
# ---------------------------------------------------------------------------
# Column order matches staging_schema.sql. The natural key is event_id.
EARTHQUAKE_COLUMNS = [
    "event_id", "mag", "mag_type", "place", "event_time",
    "latitude", "longitude", "depth",
    "status", "sig", "net", "nst", "tsunami", "alert", "cdi", "mmi",
    "gap", "dmin", "rms", "felt", "event_type", "title",
    "event_url", "detail_url", "updated",
]

# GeoJSON geometry is [longitude, latitude, depth].
def _flatten_earthquake(record: dict) -> tuple:
    geom = record.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None, None]
    # The model exposes latitude/longitude/depth explicitly; fall back to the
    # geometry coordinates for records that bypass the model (e.g. tests).
    latitude = record.get("latitude", coords[1] if len(coords) > 1 else None)
    longitude = record.get("longitude", coords[0] if len(coords) > 0 else None)
    depth = record.get("depth", coords[2] if len(coords) > 2 else None)
    values = {
        "event_id": record.get("id"),
        "mag": record.get("mag"),
        "mag_type": record.get("mag_type"),
        "place": record.get("place"),
        "event_time": record.get("time"),
        "latitude": latitude,
        "longitude": longitude,
        "depth": depth,
        "status": record.get("status"),
        "sig": record.get("sig"),
        "net": record.get("net"),
        "nst": record.get("nst"),
        "tsunami": record.get("tsunami"),
        "alert": record.get("alert"),
        "cdi": record.get("cdi"),
        "mmi": record.get("mmi"),
        "gap": record.get("gap"),
        "dmin": record.get("dmin"),
        "rms": record.get("rms"),
        "felt": record.get("felt"),
        "event_type": record.get("type"),
        "title": record.get("title"),
        "event_url": record.get("url"),
        "detail_url": record.get("detail"),
        "updated": record.get("updated"),
    }
    return tuple(values[c] for c in EARTHQUAKE_COLUMNS)


def _flatten(entity: str, record: dict) -> tuple:
    if entity == "earthquakes":
        return _flatten_earthquake(record)
    raise KeyError(f"Unknown entity: {entity}")


def _columns(entity: str) -> list[str]:
    if entity == "earthquakes":
        return EARTHQUAKE_COLUMNS
    raise KeyError(f"Unknown entity: {entity}")


def _upsert_sql(entity: str, batch_id: uuid.UUID) -> str:
    cols = _columns(entity)
    key_col = "event_id" if entity == "earthquakes" else "id"
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    update_assignments = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c != key_col
    )
    return (
        f"INSERT INTO staging.{entity} ({col_list}, loaded_at, batch_id) "
        f"VALUES ({placeholders}, now(), %s) "
        f"ON CONFLICT ({key_col}) DO UPDATE SET {update_assignments}, "
        f"loaded_at = now(), batch_id = %s"
    )


def load_records(records: Iterable, batch_id: uuid.UUID | None = None) -> dict[str, int]:
    """Insert/update a batch of records. Returns per-entity row counts."""
    batch_id = batch_id or uuid.uuid4()
    grouped: dict[str, list[tuple]] = {}
    for rec in records:
        grouped.setdefault(rec.entity, []).append(_flatten(rec.entity, rec.record))

    counts: dict[str, int] = {}
    from etl.db import connect

    conn = connect()
    try:
        for entity, rows in grouped.items():
            if not rows:
                continue
            sql_text = _upsert_sql(entity, batch_id)
            params = [row + (str(batch_id), str(batch_id)) for row in rows]
            n = executemany(conn, sql_text, params)
            counts[entity] = n
        return counts
    finally:
        conn.close()


def insert_errors(conn, errors: list[dict], batch_id: uuid.UUID | None = None) -> int:
    """Route source-rejected rows into staging.load_errors.

    Idempotent on (entity, source_id): a bad row is reported once and a rerun
    that re-rejects the same row does not duplicate the error row.
    """
    if not errors:
        return 0
    batch_id = batch_id or uuid.uuid4()
    rows = [
        (
            err.get("entity"),
            err.get("source_id"),
            err.get("error", ""),
            _jsonb(err.get("raw")),
            str(batch_id),
        )
        for err in errors
    ]
    sql = (
        "INSERT INTO staging.load_errors (entity, source_id, error, raw, batch_id) "
        "VALUES (%s, %s, %s, %s::jsonb, %s) "
        "ON CONFLICT (entity, source_id) DO NOTHING"
    )
    from etl.db import executemany

    return executemany(conn, sql, rows)


def _jsonb(value) -> str | None:
    import json

    if value is None:
        return None
    return json.dumps(value)


def ensure_schema(conn) -> None:
    """Apply staging_schema.sql if the staging schema/tables don't exist yet."""
    from etl.db import table_exists

    if not table_exists(conn, "staging", "earthquakes"):
        from pathlib import Path

        sql_path = Path(__file__).resolve().parent.parent.parent / "sql" / "staging_schema.sql"
        execute(conn, sql_path.read_text(encoding="utf-8"))


def load_counts(conn) -> list[dict]:
    """Current row counts per staging entity — used for verification output."""
    rows = []
    for entity in ("earthquakes",):
        row = fetchone(conn, f"SELECT count(*) AS n FROM staging.{entity}")
        rows.append({"entity": entity, "count": int(row["n"]) if row else 0})
    return rows


def error_counts(conn) -> list[dict]:
    """Current rejected-row count per entity — used for verification output."""
    rows = []
    row = fetchone(
        conn,
        "SELECT entity, count(*) AS n FROM staging.load_errors GROUP BY entity",
    )
    if row:
        rows.append({"entity": row["entity"], "count": int(row["n"])})
    return rows


# ---------------------------------------------------------------------------
# Incremental cursor (staging.load_cursors)
# ---------------------------------------------------------------------------
def read_cursors(conn) -> dict[str, int]:
    """Return {entity: last_seen_id} for every entity with a cursor row."""
    rows = fetchall(conn, "SELECT entity, last_seen_id FROM staging.load_cursors")
    return {r["entity"]: int(r["last_seen_id"]) for r in rows}


def write_cursors(conn, cursors: dict[str, int]) -> None:
    """Upsert cursor values. Callers pass only the entities they advanced."""
    for entity, last_seen_id in cursors.items():
        execute(
            conn,
            "INSERT INTO staging.load_cursors (entity, last_seen_id) VALUES (%s, %s) "
            "ON CONFLICT (entity) DO UPDATE SET last_seen_id = EXCLUDED.last_seen_id, "
            "updated_at = now()",
            (entity, last_seen_id),
        )