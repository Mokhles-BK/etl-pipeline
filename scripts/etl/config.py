"""Environment configuration, validated lazily.

Validation happens on first access via `get_config()`, NOT at import time.
This matters: Airflow imports every DAG file in `dags_folder` at scheduler and
webserver startup, in worker processes where `.env` is not loaded. A module
that raises SystemExit at import would prevent the DAG from ever parsing.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    usgs_base_url: str
    usgs_min_magnitude: float
    batch_size: int
    load_mode: str  # "upsert" | "append"

    @property
    def dsn(self) -> str:
        return (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_password}"
        )


def _require(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def load_config() -> Config:
    return Config(
        db_host=_require("DB_HOST"),
        db_port=int(_require("DB_PORT")),
        db_name=_require("DB_NAME"),
        db_user=_require("DB_USER"),
        db_password=_require("DB_PASSWORD"),
        usgs_base_url=_require("USGS_BASE_URL"),
        usgs_min_magnitude=float(os.environ.get("USGS_MIN_MAGNITUDE", "1.0")),
        batch_size=int(os.environ.get("BATCH_SIZE", "100")),
        load_mode=os.environ.get("LOAD_MODE", "upsert").lower(),
    )


@lru_cache(maxsize=1)
def get_config() -> Config:
    """Cached accessor. Safe to call from any module; validates on first use."""
    return load_config()