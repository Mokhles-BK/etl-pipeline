"""Run-log tests: every load/transform run leaves one row in monitoring.pipeline_runs.

These opt in to run logging (conftest turns it off by default) and use unique
source names so they can clean up only their own rows.
"""

from __future__ import annotations

import pytest

from etl.models import NormalizedRecord
from etl.pipeline import run_incremental
from etl.runlog import record_run, start_clock

SRC = "runlog_test_source"


def _rows(conn, source):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM monitoring.pipeline_runs WHERE source = %s ORDER BY run_id", (source,)
        )
        return cur.fetchall()


@pytest.fixture
def conn(monkeypatch):
    from etl.db import connect

    from etl.loader import read_cursors, write_cursors

    monkeypatch.setenv("ETL_RUNLOG", "on")
    c = connect()
    saved_cursors = read_cursors(c)  # snapshot: never leave the real cursor changed
    try:
        yield c
    finally:
        c.rollback()
        with c.cursor() as cur:
            cur.execute("DELETE FROM monitoring.pipeline_runs WHERE source LIKE 'runlog_test%'")
            cur.execute("DELETE FROM staging.load_cursors")
        c.commit()
        if saved_cursors:
            write_cursors(c, saved_cursors)
        c.close()


@pytest.mark.integration
def test_record_run_writes_a_row(conn):
    assert record_run(conn, start_clock(), stage="load", source=SRC, ok=True,
                      fetched=5, loaded=4, rejected=1, batch_id="b1") is True
    (row,) = _rows(conn, SRC)
    assert (row["stage"], row["ok"], row["records_fetched"], row["records_loaded"],
            row["records_rejected"], row["batch_id"]) == ("load", True, 5, 4, 1, "b1")
    assert row["duration_ms"] >= 0 and row["finished_at"] >= row["started_at"]


@pytest.mark.integration
def test_disabled_by_env_var_writes_nothing(conn, monkeypatch):
    monkeypatch.setenv("ETL_RUNLOG", "off")
    assert record_run(conn, start_clock(), stage="load", source=SRC, ok=True) is False
    assert _rows(conn, SRC) == []


@pytest.mark.integration
def test_run_incremental_logs_a_successful_run(conn):
    class OneEventSource:
        name = SRC
        cursor_field = "time"
        errors = []

        def fetch(self, since=None, entity=None):
            if entity is not None and entity != "earthquakes":
                return
            yield NormalizedRecord(
                entity="earthquakes",
                record={"id": "runlogtest1", "mag": 4.0, "mag_type": "mb", "place": "test",
                        "time": 1, "geometry": {"type": "Point", "coordinates": [-1, 2, 3]}},
            )

    from etl.loader import write_cursors

    try:
        write_cursors(conn, {"earthquakes": 0})
        report = run_incremental(OneEventSource(), conn)
        assert report.ok
        (row,) = _rows(conn, SRC)
        assert row["stage"] == "load" and row["ok"] is True
        assert row["records_fetched"] == 1 and row["records_loaded"] == 1
        assert row["batch_id"] == report.batch_id
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.earthquakes WHERE event_id = 'runlogtest1'")
        conn.commit()


@pytest.mark.integration
def test_failed_run_is_logged_with_its_error(conn):
    class BrokenSource:
        name = "runlog_test_broken"
        cursor_field = "time"
        errors = []

        def fetch(self, since=None, entity=None):
            raise RuntimeError("upstream exploded")

    from etl.loader import write_cursors

    write_cursors(conn, {"earthquakes": 0})  # incremental runs fetch per cursor row
    report = run_incremental(BrokenSource(), conn)
    assert not report.ok
    (row,) = _rows(conn, "runlog_test_broken")
    assert row["ok"] is False
    assert "upstream exploded" in row["error"]


def test_logging_failure_never_raises():
    class DeadConn:
        def rollback(self):
            raise RuntimeError("connection is dead")

        def cursor(self):
            raise RuntimeError("connection is dead")

    import os

    os.environ["ETL_RUNLOG"] = "on"
    try:
        assert record_run(DeadConn(), start_clock(), stage="load", source=SRC, ok=True) is False
    finally:
        os.environ["ETL_RUNLOG"] = "off"
