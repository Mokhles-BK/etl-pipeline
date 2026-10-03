"""Pytest configuration shared by the integration tests.

The Postgres instance reachable from this machine speaks plain TCP without a
server certificate, but the libpq default (sslmode=require) tries to negotiate
SSL and fails with "SSL error: unexpected eof while reading". Setting
PGSSLMODE=disable for the test process keeps the connection logic unchanged
while making the tests hermetic against the server's SSL setting.
"""

from __future__ import annotations

import os

os.environ.setdefault("PGSSLMODE", "disable")

import pytest


@pytest.fixture(scope="session", autouse=True)
def _apply_schemas():
    """Make the integration tests self-contained on an empty database.

    Both schema files are idempotent (CREATE ... IF NOT EXISTS), so this is a
    no-op against a database that already has them, and it lets CI run against
    a fresh Postgres service container. If no database is reachable the
    fixture does nothing, so the pure unit tests still run.
    """
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent
    try:
        from etl.db import apply_sql_file, connect

        conn = connect(retries=1, delay=0)
    except Exception:
        yield
        return
    try:
        apply_sql_file(conn, str(root / "sql" / "staging_schema.sql"))
        apply_sql_file(conn, str(root / "sql" / "warehouse_schema.sql"))
    finally:
        conn.close()
    yield
