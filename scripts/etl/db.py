"""Thin connection helper around psycopg2.

One module owns the DB so every caller shares the same DSN and the same
retry/backoff behaviour.
"""

from __future__ import annotations

import time
from typing import Any, Iterable, Sequence

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from etl.config import get_config


def connect(retries: int = 5, delay: float = 2.0):
    """Connect with retries; raise if it never succeeds."""
    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            return psycopg2.connect(get_config().dsn, cursor_factory=RealDictCursor)
        except psycopg2.OperationalError as exc:  # pragma: no cover - depends on env
            last_exc = exc
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Postgres after {retries} attempts") from last_exc


def execute(conn, query: str, params: Sequence[Any] | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()


def executemany(conn, query: str, params_list: Iterable[Sequence[Any]]) -> int:
    with conn.cursor() as cur:
        cur.executemany(query, params_list)
    conn.commit()
    return cur.rowcount


def fetchall(conn, query: str, params: Sequence[Any] | None = None) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def fetchone(conn, query: str, params: Sequence[Any] | None = None) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()


def table_exists(conn, schema: str, table: str) -> bool:
    row = fetchone(
        conn,
        "SELECT to_regclass(%s) AS rel",
        (f"{schema}.{table}",),
    )
    return bool(row and row["rel"])


def apply_sql_file(conn, path: str) -> None:
    with open(path, encoding="utf-8") as fh:
        sql_text = fh.read()
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()


def qualified(name: str) -> sql.Composed:
    """Build a safe, schema-qualified identifier (staging.<name>)."""
    return sql.SQL("staging.{}").format(sql.Identifier(name))