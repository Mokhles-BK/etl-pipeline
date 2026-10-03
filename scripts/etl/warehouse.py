"""Warehouse transform: staging.earthquakes -> warehouse star schema.

Run manually with `python -m etl.warehouse` after a load cycle. Every step is
idempotent — dimensions are inserted with ON CONFLICT DO NOTHING (a dimension
row, once created, never needs updating for this dataset), and the fact table
upserts on event_id, same convention as staging_schema.sql's loader.

Design note: this reads the *entire* staging.earthquakes table on every run
rather than tracking its own watermark. That's a deliberate simplification —
the transform is cheap relative to the load itself (a handful of INSERT..SELECT
statements, not a per-row Python loop), and staying watermark-free means a
schema change or backfill never requires resetting a second cursor. If the
staging table grows very large, switch WHERE clauses below to filter on
loaded_at against a warehouse-side watermark.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from etl.db import execute, fetchone, table_exists

log = logging.getLogger("etl.warehouse")


@dataclass
class TransformReport:
    dim_time: int = 0
    dim_location: int = 0
    dim_magnitude_type: int = 0
    fact_earthquake_events: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def ensure_warehouse_schema(conn) -> None:
    """Apply warehouse_schema.sql if the warehouse tables don't exist yet."""
    if not table_exists(conn, "warehouse", "fact_earthquake_events"):
        sql_path = Path(__file__).resolve().parent.parent.parent / "sql" / "warehouse_schema.sql"
        execute(conn, sql_path.read_text(encoding="utf-8"))


def _run_and_count(conn, sql_text: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql_text)
        n = cur.rowcount
    conn.commit()
    return max(n, 0)


def transform_dim_time(conn) -> int:
    """One row per distinct calendar date present in staging.earthquakes."""
    sql_text = """
        INSERT INTO warehouse.dim_time (date_key, full_date, year, month, day, day_of_week, month_name)
        SELECT
            (to_char(d.full_date, 'YYYYMMDD'))::INTEGER AS date_key,
            d.full_date,
            EXTRACT(YEAR FROM d.full_date)::SMALLINT,
            EXTRACT(MONTH FROM d.full_date)::SMALLINT,
            EXTRACT(DAY FROM d.full_date)::SMALLINT,
            EXTRACT(DOW FROM d.full_date)::SMALLINT,
            to_char(d.full_date, 'FMMonth')
        FROM (
            SELECT DISTINCT to_timestamp(event_time / 1000.0)::date AS full_date
            FROM staging.earthquakes
        ) d
        ON CONFLICT (date_key) DO NOTHING;
    """
    return _run_and_count(conn, sql_text)


def transform_dim_location(conn) -> int:
    """One row per (region, 1-degree lat/lon bucket).

    Region is parsed from the USGS `place` string: the text after the last
    comma (e.g. "Indonesia" from "126 km NNE of Teluknaga, Indonesia"), falling
    back to the whole place string when there's no comma.
    """
    sql_text = """
        INSERT INTO warehouse.dim_location (region, lat_bucket, lon_bucket)
        SELECT DISTINCT
            COALESCE(NULLIF(TRIM(split_part(place, ',', -1)), ''), place, 'Unknown') AS region,
            FLOOR(latitude)::INTEGER AS lat_bucket,
            FLOOR(longitude)::INTEGER AS lon_bucket
        FROM staging.earthquakes
        ON CONFLICT (region, lat_bucket, lon_bucket) DO NOTHING;
    """
    return _run_and_count(conn, sql_text)


def transform_dim_magnitude_type(conn) -> int:
    sql_text = """
        INSERT INTO warehouse.dim_magnitude_type (mag_type)
        SELECT DISTINCT COALESCE(mag_type, 'unknown')
        FROM staging.earthquakes
        ON CONFLICT (mag_type) DO NOTHING;
    """
    return _run_and_count(conn, sql_text)


def transform_fact_earthquake_events(conn) -> int:
    """Upsert one fact row per staging.earthquakes row, resolving dimension
    foreign keys by joining on each dimension's natural key. Dimensions must
    be populated first (see run_transform)."""
    sql_text = """
        INSERT INTO warehouse.fact_earthquake_events (
            event_id, date_key, location_key, mag_type_key,
            mag, depth, sig, felt, nst, gap, dmin, rms, tsunami, event_time
        )
        SELECT
            s.event_id,
            dt.date_key,
            dl.location_key,
            dm.mag_type_key,
            s.mag, s.depth, s.sig, s.felt, s.nst, s.gap, s.dmin, s.rms, s.tsunami,
            s.event_time
        FROM staging.earthquakes s
        JOIN warehouse.dim_time dt
            ON dt.full_date = to_timestamp(s.event_time / 1000.0)::date
        LEFT JOIN warehouse.dim_location dl
            ON dl.region = COALESCE(NULLIF(TRIM(split_part(s.place, ',', -1)), ''), s.place, 'Unknown')
           AND dl.lat_bucket = FLOOR(s.latitude)::INTEGER
           AND dl.lon_bucket = FLOOR(s.longitude)::INTEGER
        LEFT JOIN warehouse.dim_magnitude_type dm
            ON dm.mag_type = COALESCE(s.mag_type, 'unknown')
        ON CONFLICT (event_id) DO UPDATE SET
            date_key = EXCLUDED.date_key,
            location_key = EXCLUDED.location_key,
            mag_type_key = EXCLUDED.mag_type_key,
            mag = EXCLUDED.mag,
            depth = EXCLUDED.depth,
            sig = EXCLUDED.sig,
            felt = EXCLUDED.felt,
            nst = EXCLUDED.nst,
            gap = EXCLUDED.gap,
            dmin = EXCLUDED.dmin,
            rms = EXCLUDED.rms,
            tsunami = EXCLUDED.tsunami,
            event_time = EXCLUDED.event_time,
            loaded_at = now();
    """
    return _run_and_count(conn, sql_text)


def run_transform(conn) -> TransformReport:
    """Run the full staging -> warehouse transform. Dimensions before fact,
    since the fact upsert joins against them to resolve foreign keys."""
    report = TransformReport()
    try:
        ensure_warehouse_schema(conn)
        report.dim_time = transform_dim_time(conn)
        report.dim_location = transform_dim_location(conn)
        report.dim_magnitude_type = transform_dim_magnitude_type(conn)
        report.fact_earthquake_events = transform_fact_earthquake_events(conn)
        log.info(
            "warehouse transform: dim_time=%d dim_location=%d dim_magnitude_type=%d fact=%d",
            report.dim_time, report.dim_location, report.dim_magnitude_type,
            report.fact_earthquake_events,
        )
    except Exception as exc:  # pragma: no cover - surfaced via report
        report.errors.append(f"{type(exc).__name__}: {exc}")
        log.exception("warehouse transform failed")
    return report


def main() -> int:
    import logging as _logging

    from etl.db import connect

    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    conn = connect()
    try:
        report = run_transform(conn)
    finally:
        conn.close()

    print(f"dim_time:             {report.dim_time}")
    print(f"dim_location:         {report.dim_location}")
    print(f"dim_magnitude_type:   {report.dim_magnitude_type}")
    print(f"fact_earthquake_events: {report.fact_earthquake_events}")
    if report.errors:
        print("errors:", *report.errors, sep="\n  ")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
