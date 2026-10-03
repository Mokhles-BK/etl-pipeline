"""Pipeline orchestrator: fetch -> validate -> load -> report.

Run manually or from a scheduler. Every step is logged and the final
report is returned so a caller (CLI, test, or later scheduler) can assert
on the outcome.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Iterator

from etl.db import connect
from etl.loader import (
    ensure_schema,
    error_counts,
    insert_errors,
    load_counts,
    load_records,
    read_cursors,
    write_cursors,
)
from etl.models import NormalizedRecord
from etl.sources import Source

log = logging.getLogger("etl.pipeline")


@dataclass
class LoadReport:
    batch_id: str
    records_fetched: int = 0
    records_loaded: dict[str, int] = field(default_factory=dict)
    errors_loaded: int = 0
    errors: list[str] = field(default_factory=list)
    db_counts: list[dict] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        # A load that fetches nothing is a successful no-op for incremental
        # runs — the source simply has no newer records than the cursor.
        return not self.errors


def run(source: Source, since: int | None = None, batch_id: uuid.UUID | None = None) -> LoadReport:
    """Execute one full load cycle for the given source."""
    batch_id = batch_id or uuid.uuid4()
    report = LoadReport(batch_id=str(batch_id))

    conn = None
    try:
        conn = connect()
        ensure_schema(conn)

        records: Iterator[NormalizedRecord] = source.fetch(since=since)
        # Materialize so we can count fetched records before the DB write.
        buffered = list(records)
        report.records_fetched = len(buffered)
        log.info("fetched %d records from %s", report.records_fetched, source.name)

        # Source validation failures are collected on the source, not raised.
        bad = getattr(source, "errors", None) or []
        if bad:
            report.errors_loaded = insert_errors(conn, bad, batch_id=batch_id)
            log.info("routed %d rejected rows to staging.load_errors", report.errors_loaded)

        if buffered:
            report.records_loaded = load_records(buffered, batch_id=batch_id)
        report.db_counts = load_counts(conn)
        report.error_counts = error_counts(conn)
    except Exception as exc:  # pragma: no cover - surfaced via report
        report.errors.append(f"{type(exc).__name__}: {exc}")
        log.exception("load failed")
    finally:
        if conn is not None:
            conn.close()

    return report


def run_incremental(source: Source, conn, batch_id: uuid.UUID | None = None) -> LoadReport:
    """Load only records newer than the per-entity cursor, then advance it.

    Returns a report whose `ok` is True even when the source has nothing new
    (a no-op run is not a failure). The cursor lives in staging.load_cursors so
    it survives scheduler restarts and is independent of the scheduler's state.

    The cursor field is per-source (default "id"; USGS overrides to "time").
    """
    batch_id = batch_id or uuid.uuid4()
    report = LoadReport(batch_id=str(batch_id))

    try:
        ensure_schema(conn)
        cursors = read_cursors(conn)
        cursor_field = getattr(source, "cursor_field", "id")
        # One cursor per entity: a global max would skip records whose value
        # falls between entities (e.g. an event id that no other entity has).
        for entity, cursor in cursors.items():
            records = list(source.fetch(since=cursor, entity=entity))
            report.records_fetched += len(records)
            if records:
                loaded = load_records(records, batch_id=batch_id)
                report.records_loaded.update(loaded)
                # Advance the cursor past the highest value we just loaded.
                max_id = max(r.record[cursor_field] for r in records)
                write_cursors(conn, {entity: max_id})
        # Route validation failures to staging.load_errors.
        bad = getattr(source, "errors", None) or []
        if bad:
            report.errors_loaded = insert_errors(conn, bad, batch_id=batch_id)
        report.db_counts = load_counts(conn)
        report.error_counts = error_counts(conn)
    except Exception as exc:  # pragma: no cover - surfaced via report
        report.errors.append(f"{type(exc).__name__}: {exc}")
        log.exception("incremental load failed")

    return report