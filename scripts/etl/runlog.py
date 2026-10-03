"""Run log: one row in monitoring.pipeline_runs per load or transform run.

Rules:
  * Logging must never break or mask a pipeline run: every failure here is
    swallowed and logged, never raised.
  * Set ETL_RUNLOG=off to disable (the test suite does this so tests do not
    pollute the real run history).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger("etl.runlog")

_SCHEMA_FILE = Path(__file__).resolve().parent.parent.parent / "sql" / "monitoring_schema.sql"


@dataclass(frozen=True)
class RunClock:
    started_at: datetime
    t0: float


def start_clock() -> RunClock:
    return RunClock(started_at=datetime.now(timezone.utc), t0=time.monotonic())


def _enabled() -> bool:
    return os.environ.get("ETL_RUNLOG", "on").strip().lower() != "off"


def ensure_monitoring_schema(conn) -> None:
    from etl.db import apply_sql_file, table_exists

    if not table_exists(conn, "monitoring", "pipeline_runs"):
        apply_sql_file(conn, str(_SCHEMA_FILE))


def record_run(
    conn,
    clock: RunClock,
    *,
    stage: str,
    source: str | None,
    ok: bool,
    fetched: int = 0,
    loaded: int = 0,
    rejected: int = 0,
    error: str | None = None,
    batch_id: str | None = None,
) -> bool:
    """Insert one run row. Returns True if written, False if skipped or failed."""
    if not _enabled():
        return False
    finished_at = datetime.now(timezone.utc)
    duration_ms = int((time.monotonic() - clock.t0) * 1000)
    try:
        conn.rollback()  # clear any aborted transaction left by a failed run
        ensure_monitoring_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO monitoring.pipeline_runs
                    (stage, source, batch_id, started_at, finished_at, duration_ms,
                     ok, records_fetched, records_loaded, records_rejected, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (stage, source, batch_id, clock.started_at, finished_at, duration_ms,
                 ok, fetched, loaded, rejected, error[:2000] if error else None),
            )
        conn.commit()
        return True
    except Exception as exc:  # noqa: BLE001 - logging must never raise
        log.warning("could not write run log: %s", exc)
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        return False


def finish_load(conn, clock: RunClock, source_name: str | None, report: Any) -> bool:
    """Record a load run from a LoadReport."""
    return record_run(
        conn,
        clock,
        stage="load",
        source=source_name,
        ok=report.ok,
        fetched=report.records_fetched,
        loaded=sum(report.records_loaded.values()) if report.records_loaded else 0,
        rejected=report.errors_loaded,
        error="; ".join(report.errors) if report.errors else None,
        batch_id=report.batch_id,
    )


def finish_transform(conn, clock: RunClock, report: Any) -> bool:
    """Record a warehouse transform run from a TransformReport."""
    return record_run(
        conn,
        clock,
        stage="transform",
        source="warehouse",
        ok=not report.errors,
        loaded=report.fact_earthquake_events,
        error="; ".join(report.errors) if report.errors else None,
    )
