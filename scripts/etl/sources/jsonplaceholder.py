"""JSONPlaceholder source.

Committed data source (no API key required). Yields normalized records for
three entities: users, posts, todos. Each entity is fetched as a single
page since JSONPlaceholder has no pagination — 100 rows each, small enough
to load in one shot but still batched on insert for transactional safety.
"""

from __future__ import annotations

from typing import Iterator

import requests

from etl.config import get_config
from etl.models import NormalizedRecord, Post, Todo, User
from etl.sources import Source


class JsonPlaceholder(Source):
    name = "jsonplaceholder"

    def __init__(self, base_url: str | None = None, timeout: float = 30.0) -> None:
        self.base_url = (base_url or get_config().base_url).rstrip("/")
        self.timeout = timeout

    def _get(self, path: str) -> list[dict]:
        resp = requests.get(f"{self.base_url}{path}", timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected payload shape for {path}: {type(data)}")
        return data

    # -- Source interface ---------------------------------------------------

    def entities(self) -> list[str]:
        return ["users", "posts", "todos"]

    def fetch(self, since: int | None = None, entity: str | None = None) -> Iterator[NormalizedRecord]:
        for ent in self.entities():
            if entity is not None and ent != entity:
                continue
            yield from self._fetch_entity(ent, since)

    # -- internal -----------------------------------------------------------

    def _fetch_entity(self, entity: str, since: int | None) -> Iterator[NormalizedRecord]:
        raw = self._get(f"/{entity}")
        for row in raw:
            record = self._validate(entity, row)
            if since is not None and record["id"] <= since:
                continue
            yield NormalizedRecord(entity=entity, record=record)

    @staticmethod
    def _validate(entity: str, row: dict) -> dict:
        # Validate through pydantic so a malformed record fails at the edge;
        # the returned dict is the canonical shape the loader writes.
        model_map = {"users": User, "posts": Post, "todos": Todo}
        model = model_map[entity]
        parsed = model.model_validate(row)
        return parsed.model_dump()