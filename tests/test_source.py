"""Tests for the USGS Earthquake source, using a local mock server.

We use a tiny WSGI/HTTP server to avoid depending on network availability in
CI. The source only depends on requests.get, so monkeypatching the URL to
localhost is sufficient.
"""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from etl.sources.usgs_earthquakes import USGSEarthquakes


def _feature(eid: str, mag: float, t: int, lon: float = -122.0, lat: float = 37.0, depth: float = 10.0) -> dict:
    return {
        "type": "Feature",
        "id": eid,
        "geometry": {"type": "Point", "coordinates": [lon, lat, depth]},
        "properties": {
            "mag": mag,
            "magType": "mb",
            "place": "test",
            "time": t,
            "status": "reviewed",
            "sig": 100,
            "net": "us",
            "title": f"M {mag} - test",
        },
    }


@pytest.fixture
def fake_server():
    payload = {
        "features": [
            _feature("us1", 4.0, 1000),
            _feature("us2", 4.5, 2000),
            _feature("us3", 5.0, 3000),
        ],
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = json.dumps({"type": "FeatureCollection", "features": payload["features"]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # silence test noise
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = __import__("threading").Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_entities(fake_server):
    src = USGSEarthquakes(base_url=fake_server)
    assert src.entities() == ["earthquakes"]


def test_fetch_yields_normalized_records(fake_server):
    src = USGSEarthquakes(base_url=fake_server)
    records = list(src.fetch())
    assert len(records) == 3
    assert all(r.entity == "earthquakes" for r in records)
    assert records[0].record["id"] == "us1"
    assert records[0].record["mag"] == 4.0
    assert records[0].record["time"] == 1000
    assert records[0].record["latitude"] == 37.0
    assert records[0].record["longitude"] == -122.0
    assert records[0].record["depth"] == 10.0
    assert records[0].record["mag_type"] == "mb"


def test_fetch_since_cursor(fake_server):
    src = USGSEarthquakes(base_url=fake_server)
    # only events with time > 1500
    records = list(src.fetch(since=1500))
    assert [r.record["time"] for r in records] == [2000, 3000]


def test_fetch_raises_on_bad_payload(fake_server):
    class BadHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = json.dumps({"not": "a featurecollection"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), BadHandler)
    port = server.server_address[1]
    t = __import__("threading").Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        src = USGSEarthquakes(base_url=f"http://127.0.0.1:{port}")
        with pytest.raises(ValueError):
            list(src.fetch())
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_fetch_routes_bad_rows_to_errors(fake_server):
    """Rows that fail pydantic validation are collected, not raised."""
    bad_payload = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "id": "us_bad", "geometry": None, "properties": {"mag": None, "place": "x", "time": 1}},
        ],
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = json.dumps(bad_payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    t = __import__("threading").Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        src = USGSEarthquakes(base_url=f"http://127.0.0.1:{port}")
        records = list(src.fetch())
        assert records == []
        assert len(src.errors) == 1
        assert src.errors[0]["source_id"] == "us_bad"
        assert "magnitude" in src.errors[0]["error"].lower()
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_pagination():
    """The pager pages by offset until a page is shorter than the limit."""
    all_features = [_feature(f"us{i}", 4.0, i) for i in range(7)]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            # Parse offset from the query string; the server honours it.
            from urllib.parse import urlparse, parse_qs

            qs = parse_qs(urlparse(self.path).query)
            offset = int(qs.get("offset", ["1"])[0])
            limit = int(qs.get("limit", ["20000"])[0])
            page = all_features[offset - 1 : offset - 1 + limit]
            body = json.dumps(
                {"type": "FeatureCollection", "features": page}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    t = __import__("threading").Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        # limit=3: pages of 3, 3, 1 → pager stops when a page is short.
        src = USGSEarthquakes(base_url=f"http://127.0.0.1:{port}", limit=3)
        records = list(src.fetch())
        assert len(records) == 7
        assert [r.record["id"] for r in records] == [f"us{i}" for i in range(7)]
    finally:
        server.shutdown()
        t.join(timeout=5)