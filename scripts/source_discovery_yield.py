#!/usr/bin/env python3
"""Report curated source review yield by discovery channel."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_discovery_yield import (  # noqa: E402
    build_source_discovery_yield_report,
    format_source_discovery_yield_csv,
    format_source_discovery_yield_json,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--source-type",
        help="Restrict to one curated_sources.source_type.",
    )
    parser.add_argument(
        "--discovery-source",
        help="Restrict to one curated_sources.discovery_source; use 'unknown' for null/blank.",
    )
    parser.add_argument(
        "--min-samples",
        type=_non_negative_int,
        default=0,
        help="Minimum curated_sources.sample_count to include (default: 0).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (default: json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_source_discovery_yield_report(
                    conn,
                    source_type=args.source_type,
                    discovery_source=args.discovery_source,
                    min_samples=args.min_samples,
                )
        else:
            with script_context() as (_config, db):
                report = build_source_discovery_yield_report(
                    db,
                    source_type=args.source_type,
                    discovery_source=args.discovery_source,
                    min_samples=args.min_samples,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        print(format_source_discovery_yield_csv(report))
    else:
        print(format_source_discovery_yield_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
