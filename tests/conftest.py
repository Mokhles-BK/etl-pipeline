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