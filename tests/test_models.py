"""Tests for the Pydantic models and the loader's flatten/upsert logic.

These run without a live DB — the loader's _flatten and _upsert_sql are pure
functions, so we can assert on the generated SQL and row tuples.
"""

import uuid

import pytest

from etl.loader import _flatten, _upsert_sql
from etl.models import Earthquake, NormalizedRecord


def test_earthquake_model_validates_geojson_feature():
    feature = {
        "type": "Feature",
        "id": "us6000twys",
        "geometry": {"type": "Point", "coordinates": [-75.7814, 3.948, 10]},
        "properties": {
            "mag": 4.6,
            "magType": "mb",
            "place": "46 km E of Union, Philippines",
            "time": 1790109283296,
            "status": "reviewed",
            "sig": 326,
            "net": "us",
            "title": "M 4.6 - 46 km E of Union, Philippines",
        },
    }
    eq = Earthquake.model_validate(feature)
    dumped = eq.model_dump()
    assert dumped["id"] == "us6000twys"
    assert dumped["mag"] == 4.6
    assert dumped["mag_type"] == "mb"
    assert dumped["time"] == 1790109283296
    assert dumped["latitude"] == 3.948
    assert dumped["longitude"] == -75.7814
    assert dumped["depth"] == 10.0
    # extra="allow" keeps API-only fields
    assert "sig" in dumped
    assert "net" in dumped


def test_earthquake_model_rejects_null_magnitude():
    with pytest.raises(Exception):
        Earthquake.model_validate(
            {
                "type": "Feature",
                "id": "us_bad",
                "geometry": {"type": "Point", "coordinates": [-1, 2, 3]},
                "properties": {"mag": None, "place": "x", "time": 1},
            }
        )


def test_earthquake_model_requires_id_and_time():
    with pytest.raises(Exception):
        Earthquake.model_validate(
            {
                "type": "Feature",
                "properties": {"mag": 4.0, "time": 1},
            }
        )


def test_earthquake_model_drops_unknown_geometry():
    # geometry is optional; a feature without it still validates
    eq = Earthquake.model_validate(
        {
            "type": "Feature",
            "id": "us_nogeo",
            "properties": {"mag": 4.0, "place": "x", "time": 1},
        }
    )
    assert eq.geometry is None


def test_normalized_record_tags_entity():
    rec = NormalizedRecord(entity="earthquakes", record={"id": "us1", "mag": 4.0, "time": 1})
    assert rec.entity == "earthquakes"
    assert rec.record["id"] == "us1"


def test_flatten_earthquake():
    row = {
        "id": "us6000twys",
        "mag": 4.6,
        "mag_type": "mb",
        "place": "test",
        "time": 1790109283296,
        "geometry": {"type": "Point", "coordinates": [-75.7814, 3.948, 10]},
        "status": "reviewed",
        "sig": 326,
        "net": "us",
        "title": "M 4.6 - test",
    }
    flat = _flatten("earthquakes", row)
    assert flat[0] == "us6000twys"
    assert flat[1] == 4.6
    assert flat[2] == "mb"
    assert flat[4] == 1790109283296
    # Column order: event_id, mag, mag_type, place, event_time,
    # latitude, longitude, depth, ...
    assert flat[5] == 3.948   # latitude
    assert flat[6] == -75.7814  # longitude
    assert flat[7] == 10.0   # depth


def test_upsert_sql_uses_on_conflict_do_update():
    sql_text = _upsert_sql("earthquakes", uuid.UUID("12345678-1234-5678-1234-567812345678"))
    assert "INSERT INTO staging.earthquakes" in sql_text
    assert "ON CONFLICT (event_id) DO UPDATE" in sql_text
    assert "loaded_at = now()" in sql_text
    # event_id is the natural key and must NOT be in the SET list
    assert "EXCLUDED.event_id" not in sql_text
    # mag is a non-key column and must be updatable
    assert "mag = EXCLUDED.mag" in sql_text


def test_upsert_sql_includes_batch_id():
    sql_text = _upsert_sql("earthquakes", uuid.uuid4())
    assert "batch_id = %s" in sql_text