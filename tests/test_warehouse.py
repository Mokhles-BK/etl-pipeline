"""Warehouse transform tests.

Mirrors the pattern in test_idempotency.py: seed a couple of known staging
rows, run the transform twice, and assert the second run doesn't duplicate
anything in either the dimensions or the fact table.
"""
from __future__ import annotations

import pytest


@pytest.mark.integration
def test_transform_is_idempotent():
    from etl.db import connect
    from etl.warehouse import run_transform

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.earthquakes WHERE event_id >= 'wh_test'")
            cur.execute(
                "INSERT INTO staging.earthquakes "
                "(event_id, mag, mag_type, place, event_time, latitude, longitude, depth) "
                "VALUES "
                "('wh_test1', 5.1, 'mww', '10 km N of Testville, Testland', 1700000000000, 12.34, 56.78, 10.0), "
                "('wh_test2', 3.2, 'ml', '5 km S of Testville, Testland', 1700000100000, 12.30, 56.70, 5.0)"
            )
        conn.commit()

        first = run_transform(conn)
        assert first.ok, first.errors
        second = run_transform(conn)
        assert second.ok, second.errors

        # Second run should insert zero *new* dimension rows for data that's
        # already there (ON CONFLICT DO NOTHING means rowcount 0 on repeats).
        assert second.dim_time == 0
        assert second.dim_location == 0
        assert second.dim_magnitude_type == 0

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) AS n FROM warehouse.fact_earthquake_events "
                "WHERE event_id IN ('wh_test1', 'wh_test2')"
            )
            n = cur.fetchone()["n"]
        assert n == 2, "expected exactly one fact row per staging row, no duplicates"
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM warehouse.fact_earthquake_events WHERE event_id >= 'wh_test'")
            cur.execute("DELETE FROM staging.earthquakes WHERE event_id >= 'wh_test'")
        conn.commit()
        conn.close()


@pytest.mark.integration
def test_transform_resolves_dimension_foreign_keys():
    """A fact row's region/mag_type should match what was actually loaded,
    proving the dimension joins resolve to the right natural key, not just
    any row."""
    from etl.db import connect
    from etl.warehouse import run_transform

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.earthquakes WHERE event_id = 'wh_fk_test'")
            cur.execute(
                "INSERT INTO staging.earthquakes "
                "(event_id, mag, mag_type, place, event_time, latitude, longitude, depth) "
                "VALUES ('wh_fk_test', 6.0, 'mww', '1 km E of Nowhere, Freedonia', 1700000200000, 1.0, 2.0, 3.0)"
            )
        conn.commit()

        report = run_transform(conn)
        assert report.ok, report.errors

        with conn.cursor() as cur:
            cur.execute(
                "SELECT dl.region, dm.mag_type "
                "FROM warehouse.fact_earthquake_events f "
                "JOIN warehouse.dim_location dl ON dl.location_key = f.location_key "
                "JOIN warehouse.dim_magnitude_type dm ON dm.mag_type_key = f.mag_type_key "
                "WHERE f.event_id = 'wh_fk_test'"
            )
            row = cur.fetchone()
        assert row is not None
        assert row["region"] == "Freedonia"
        assert row["mag_type"] == "mww"
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM warehouse.fact_earthquake_events WHERE event_id = 'wh_fk_test'")
            cur.execute("DELETE FROM staging.earthquakes WHERE event_id = 'wh_fk_test'")
        conn.commit()
        conn.close()
