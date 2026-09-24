"""Idempotency tests.

The core guarantee of this pipeline: rerunning a load never duplicates rows.
We verify this two ways:
1. The generated SQL uses ON CONFLICT (event_id) DO UPDATE (pure, no DB).
2. An integration test that loads the same records twice against the live
   staging DB and asserts the row count is unchanged on the second run.
"""

import uuid

import pytest

from etl.loader import _upsert_sql, load_records
from etl.models import NormalizedRecord


def test_upsert_sql_conflicts_on_event_id():
    sql_text = _upsert_sql("earthquakes", uuid.uuid4())
    assert "ON CONFLICT (event_id) DO UPDATE" in sql_text
    # The natural key is the source id — that's what the conflict targets.
    assert "ON CONFLICT (event_id)" in sql_text


def test_upsert_sql_updates_non_key_columns():
    sql_text = _upsert_sql("earthquakes", uuid.uuid4())
    assert "mag = EXCLUDED.mag" in sql_text
    assert "place = EXCLUDED.place" in sql_text
    assert "event_time = EXCLUDED.event_time" in sql_text


@pytest.mark.integration
def test_double_load_is_idempotent():
    """Loading the same records twice yields the same row count both times."""
    from etl.db import connect
    from etl.loader import load_counts

    records = [
        NormalizedRecord(
            entity="earthquakes",
            record={
                "id": "us9001", "mag": 4.0, "mag_type": "mb", "place": "a",
                "time": 9001, "geometry": {"type": "Point", "coordinates": [-1, 2, 3]},
            },
        ),
        NormalizedRecord(
            entity="earthquakes",
            record={
                "id": "us9002", "mag": 4.5, "mag_type": "ml", "place": "b",
                "time": 9002, "geometry": {"type": "Point", "coordinates": [-4, 5, 6]},
            },
        ),
    ]

    conn = connect()
    try:
        # Clean slate for these test ids so the test is hermetic.
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.earthquakes WHERE event_id >= 'us9000'")
        conn.commit()

        first = load_records(records, batch_id=uuid.UUID("11111111-1111-1111-1111-111111111111"))
        second = load_records(records, batch_id=uuid.UUID("22222222-2222-2222-2222-222222222222"))

        assert first == second, f"counts differ: {first} vs {second}"
        # Count only this test's own ids — the table may hold rows from other
        # loads, so the global count is not a valid assertion here.
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM staging.earthquakes WHERE event_id >= 'us9000'")
            n = cur.fetchone()["n"]
        assert n == 2

        # No duplicate natural keys.
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM staging.earthquakes WHERE event_id >= 'us9000'")
            assert cur.fetchone()["n"] == 2
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.earthquakes WHERE event_id >= 'us9000'")
        conn.commit()
        conn.close()


@pytest.mark.integration
def test_rejected_rows_routed_to_load_errors():
    """A row that fails validation is recorded in staging.load_errors."""
    from etl.db import connect
    from etl.loader import insert_errors

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM staging.load_errors WHERE source_id IN ('us_bad1', 'us_bad2')"
            )
        conn.commit()

        errors = [
            {
                "entity": "earthquakes",
                "source_id": "us_bad1",
                "error": "magnitude is required",
                "raw": {"id": "us_bad1", "properties": {"mag": None}},
            },
            {
                "entity": "earthquakes",
                "source_id": "us_bad2",
                "error": "missing id",
                "raw": {"properties": {"mag": 4.0}},
            },
        ]
        n = insert_errors(conn, errors, batch_id=uuid.uuid4())
        assert n == 2

        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_id, error FROM staging.load_errors "
                "WHERE source_id IN ('us_bad1', 'us_bad2') ORDER BY source_id"
            )
            rows = cur.fetchall()
        assert {r["source_id"] for r in rows} == {"us_bad1", "us_bad2"}

        # Idempotent: re-inserting the same bad rows does not duplicate.
        n2 = insert_errors(conn, errors, batch_id=uuid.uuid4())
        assert n2 == 0
    finally:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM staging.load_errors WHERE source_id IN ('us_bad1', 'us_bad2')"
            )
        conn.commit()
        conn.close()