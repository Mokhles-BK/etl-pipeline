"""CLI entry point: `python -m etl.cli` runs one load cycle."""

from __future__ import annotations

import argparse
import logging
import sys

from etl.config import get_config
from etl.pipeline import run
from etl.sources.usgs_earthquakes import USGSEarthquakes


def main(argv: list[str] | None = None) -> int:
    config = get_config()
    parser = argparse.ArgumentParser(description="Run the ETL pipeline load cycle.")
    parser.add_argument(
        "--source",
        default="usgs_earthquakes",
        help="Source name (usgs_earthquakes)",
    )
    parser.add_argument(
        "--since",
        type=int,
        default=None,
        help="Cursor: only load events with time > this (epoch ms)",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.source != "usgs_earthquakes":
        print(f"Unknown source: {args.source}", file=sys.stderr)
        return 2

    report = run(USGSEarthquakes(), since=args.since)
    print(f"batch_id: {report.batch_id}")
    print(f"fetched:  {report.records_fetched}")
    for entity, count in report.records_loaded.items():
        print(f"  {entity}: {count}")
    if report.errors_loaded:
        print(f"  rejected: {report.errors_loaded}")
    print("db_counts:", {row["entity"]: row["count"] for row in report.db_counts})
    if report.error_counts:
        print("load_errors:", {row["entity"]: row["count"] for row in report.error_counts})
    if report.errors:
        print("errors:", *report.errors, sep="\n  ", file=sys.stderr)
        return 1
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())