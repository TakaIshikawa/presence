#!/usr/bin/env python3
"""Report content sources prioritized by freshness and detect stale ingestion."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_source_freshness import (  # noqa: E402
    DEFAULT_STALE_THRESHOLD_DAYS,
    build_content_source_freshness_report,
    format_content_source_freshness_csv,
    format_content_source_freshness_json,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
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
        "--stale-threshold-days",
        type=_positive_int,
        default=DEFAULT_STALE_THRESHOLD_DAYS,
        help=f"Days without ingestion before marking source as stale (default: {DEFAULT_STALE_THRESHOLD_DAYS}).",
    )
    parser.add_argument(
        "--source-type",
        help="Filter by source type (e.g., 'claude_messages', 'github_commits', 'knowledge').",
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
                report = build_content_source_freshness_report(
                    conn,
                    stale_threshold_days=args.stale_threshold_days,
                    source_type=args.source_type,
                )
        else:
            with script_context() as (_config, db):
                report = build_content_source_freshness_report(
                    db,
                    stale_threshold_days=args.stale_threshold_days,
                    source_type=args.source_type,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        print(format_content_source_freshness_csv(report))
    else:
        print(format_content_source_freshness_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
