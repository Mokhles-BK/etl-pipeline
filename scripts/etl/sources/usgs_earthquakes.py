"""USGS Earthquake source.

Committed data source (no API key required). Queries the USGS FDSNWS event
feed (https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson) and
yields normalized earthquake records.

Pagination: the feed supports `limit` (max 20000) and 1-based `offset`.
We page by incrementing offset until a page returns fewer than `limit`
features. The default window (no starttime/endtime) is the last 30 days,
which at M1.0+ is ~7500 events -- a single page at limit=20000 covers it,
but the pager handles larger windows correctly.

Incremental cursor: `since` is a Unix epoch-millis timestamp (the max event
`time` loaded so far, stored in staging.load_cursors). The feed only accepts
ISO-8601 `starttime`, so the cursor is converted; records with
`time <= since` are then dropped on the client side so the boundary event is
never duplicated.

Per-record validation happens at the edge via the Earthquake pydantic model.
Rows that fail (missing id, non-numeric magnitude, malformed geometry) are
collected in `self.errors` rather than raised, so the pipeline can route them
to staging.load_errors without crashing the load.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Iterator

import requests
from pydantic import ValidationError

from etl.config import get_config
from etl.models import Earthquake, NormalizedRecord
from etl.sources import Source

DEFAULT_LIMIT = 20000
DEFAULT_MIN_MAGNITUDE = 1.0


def _iso(epoch_millis: int) -> str:
    """Convert epoch milliseconds to the ISO-8601 string the feed expects."""
    return (
        dt.datetime.fromtimestamp(epoch_millis / 1000.0, tz=dt.timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%S.000Z")
    )


class USGSEarthquakes(Source):
    name = "usgs_earthquakes"
    # The incremental cursor is the event `time` (epoch ms), not `id` -- the
    # ids are opaque strings and are not comparable across pages.
    cursor_field = "time"

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 30.0,
        limit: int = DEFAULT_LIMIT,
        starttime: str | None = None,
        endtime: str | None = None,
        min_magnitude: float | None = None,
    ) -> None:
        # The USGS FDSNWS query endpoint is /query; the config base URL is the
        # service root. Append /query if not already present.
        base = (base_url or get_config().usgs_base_url).rstrip("/")
        if not base.endswith("/query"):
            base = f"{base}/query"
        self.base_url = base
        self.timeout = timeout
        self.limit = limit
        self.starttime = starttime
        self.endtime = endtime
        self.min_magnitude = DEFAULT_MIN_MAGNITUDE if min_magnitude is None else min_magnitude
        # Populated during fetch(); the pipeline reads it to route bad rows.
        self.errors: list[dict] = []

    def entities(self) -> list[str]:
        return ["earthquakes"]

    def fetch(self, since: int | None = None, entity: str | None = None) -> Iterator[NormalizedRecord]:
        # Fresh error collection per fetch call; the pipeline accumulates them
        # across the per-entity loop in run_incremental.
        self.errors = []
        if entity is not None and entity != "earthquakes":
            return
        yield from self._fetch_all(since=since)

    # -- internal -----------------------------------------------------------

    def _params(self, since: int | None) -> dict[str, Any]:
        params: dict[str, Any] = {
            "format": "geojson",
            "limit": self.limit,
            "offset": 1,
        }
        if since:
            # Incremental: only events after the cursor. The feed defaults
            # endtime to "now", so we get everything from the cursor forward.
            params["starttime"] = _iso(since)
            if self.endtime:
                params["endtime"] = self.endtime
        else:
            if self.starttime:
                params["starttime"] = self.starttime
            if self.endtime:
                params["endtime"] = self.endtime
        params["minmagnitude"] = self.min_magnitude
        return params

    def _fetch_all(self, since: int | None) -> Iterator[NormalizedRecord]:
        offset = 1
        while True:
            params = self._params(since)
            params["offset"] = offset
            features = self._get(params)
            if not features:
                return
            for feat in features:
                raw_id = feat.get("id") if isinstance(feat, dict) else None
                try:
                    record = self._validate(feat)
                except ValidationError as exc:
                    self.errors.append(
                        {
                            "entity": "earthquakes",
                            "source_id": raw_id,
                            "raw": feat,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    continue
                if since and record.get("time") is not None and record["time"] <= since:
                    continue
                yield NormalizedRecord(entity="earthquakes", record=record)
            if len(features) < self.limit:
                return
            offset += self.limit

    def _get(self, params: dict[str, Any]) -> list[dict]:
        resp = requests.get(self.base_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
            raise ValueError(f"Unexpected payload shape: {type(data).__name__}")
        features = data.get("features")
        if not isinstance(features, list):
            raise ValueError("Payload is missing a 'features' list")
        return features

    @staticmethod
    def _validate(feature: dict) -> dict:
        return Earthquake.model_validate(feature).model_dump()