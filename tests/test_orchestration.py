"""Tests for the incremental orchestration logic.

These cover the cursor-driven path that the Airflow DAG uses, without
needing a live scheduler. The cursor logic is pure (read/write dicts against
a real connection), so the integration test below is the meaningful check.
"""

import uuid

import pytest

from etl.loader import load_records, read_cursors, write_cursors
from etl.models import NormalizedRecord
from etl.pipeline import run_incremental
from etl.sources.usgs_earthquakes import USGSEarthquakes


def _record(eid: str, mag: float, t: int) -> NormalizedRecord:
    return NormalizedRecord(
        entity="earthquakes",
        record={
            "id": eid, "mag": mag, "mag_type": "mb", "place": "test",
            "time": t, "geometry": {"type": "Point", "coordinates": [-1, 2, 3]},
        },
    )


@pytest.mark.integration
def test_cursor_round_trip():
    from etl.db import connect

    conn = connect()
    try:
        # Reset all cursors so the test is hermetic regardless of prior runs.
        write_cursors(conn, {"earthquakes": 0})
        write_cursors(conn, {"earthquakes": 5})
        assert read_cursors(conn)["earthquakes"] == 5
        write_cursors(conn, {"earthquakes": 20})
        assert read_cursors(conn)["earthquakes"] == 20
    finally:
        conn.close()


@pytest.mark.integration
def test_incremental_run_advances_cursors_and_is_idempotent():
    from etl.db import connect

    conn = connect()
    try:
        # Reset cursors so the test is hermetic.
        write_cursors(conn, {"earthquakes": 0})

        # A fake source that only yields events with time 1..3.
        class FakeSource:
            name = "fake"
            cursor_field = "time"

            def entities(self):
                return ["earthquakes"]

            def fetch(self, since=None, entity=None):
                if entity is not None and entity != "earthquakes":
                    return
                for t in (1, 2, 3):
                    if since is None or t > since:
                        yield NormalizedRecord(
                            entity="earthquakes",
                            record={
                                "id": f"us{t}", "mag": 4.0, "mag_type": "mb",
                                "place": "test", "time": t,
                                "geometry": {"type": "Point", "coordinates": [-1, 2, 3]},
                            },
                        )

        first = run_incremental(FakeSource(), conn)
        assert first.records_fetched == 3
        assert first.records_loaded == {"earthquakes": 3}
        assert read_cursors(conn)["earthquakes"] == 3

        # Second run: cursor is 3, source yields nothing new.
        second = run_incremental(FakeSource(), conn)
        assert second.records_fetched == 0
        assert second.ok  # no-op is not a failure
        assert read_cursors(conn)["earthquakes"] == 3  # cursor unchanged
    finally:
        # Clean up test rows and reset cursors.
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.earthquakes WHERE event_id LIKE 'us%'")
            cur.execute("DELETE FROM staging.load_cursors WHERE entity='earthquakes'")
        conn.commit()
        conn.close()


@pytest.mark.integration
def test_dag_loads_real_source_incrementally():
    """End-to-end: the real source loads, and a second run is a no-op."""
    from etl.db import connect

    conn = connect()
    try:
        write_cursors(conn, {"earthquakes": 0})

        first = run_incremental(USGSEarthquakes(), conn)
        assert first.ok
        assert first.records_fetched > 0
        cursors = read_cursors(conn)
        # Cursor advanced past the max time loaded for the entity.
        assert cursors["earthquakes"] > 0

        second = run_incremental(USGSEarthquakes(), conn)
        assert second.ok
        assert second.records_fetched == 0  # nothing newer than the cursor
        assert read_cursors(conn) == cursors  # cursors stable
    finally:
        conn.close()


def test_run_incremental_ensures_schema_before_reading_cursors(monkeypatch):
    """Regression: on a fresh DB, run_incremental must create the schema first.

    Without ensure_schema(), read_cursors() hit 'relation staging.earthquakes
    does not exist' when Airflow pointed at an empty database.
    """
    import etl.pipeline as pipeline

    calls = []

    def fake_ensure_schema(conn):
        calls.append("ensure_schema")

    def fake_read_cursors(conn):
        calls.append("read_cursors")
        return {}

    monkeypatch.setattr(pipeline, "ensure_schema", fake_ensure_schema)
    monkeypatch.setattr(pipeline, "read_cursors", fake_read_cursors)
    monkeypatch.setattr(pipeline, "load_counts", lambda conn: [])
    monkeypatch.setattr(pipeline, "error_counts", lambda conn: [])

    class EmptySource:
        name = "empty"
        errors = []

    pipeline.run_incremental(EmptySource(), conn=None)

    assert calls[:2] == ["ensure_schema", "read_cursors"]
