"""Data source abstractions.

Each source is a thin object that yields typed records. The pipeline only
depends on the interface, so swapping a source is a drop-in.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator


class Source(ABC):
    """A fetchable, paginated data source."""

    name: str = "source"
    # The record field used as the incremental cursor. The default is the
    # natural key ("id"); the USGS source overrides this to "time" because
    # its ids are opaque strings while `time` is a monotonic epoch timestamp.
    cursor_field: str = "id"

    @abstractmethod
    def fetch(self, since: int | None = None, entity: str | None = None) -> Iterator:
        """Yield normalized records.

        `since` is a cursor (e.g. last seen id). When `entity` is given, only
        records of that entity are yielded — this lets the scheduler apply a
        per-entity cursor without fetching entities it isn't advancing.
        """
        ...

    @abstractmethod
    def entities(self) -> list[str]:
        """Names of the entities this source produces, e.g. ['users','posts']."""
        ...