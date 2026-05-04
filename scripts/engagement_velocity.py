#!/usr/bin/env python3
"""Report engagement velocity and identify trending interaction patterns."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.engagement_velocity import (  # noqa: E402
    DEFAULT_WINDOW_DAYS,
    build_engagement_velocity_report,
    format_engagement_velocity_csv,
    format_engagement_velocity_json,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--window-days",
        type=_positive_int,
        default=DEFAULT_WINDOW_DAYS,
        help=f"Time window in days for velocity calculation (default: {DEFAULT_WINDOW_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        help="Filter by platform (e.g., 'x', 'linkedin', 'bluesky', 'mastodon').",
    )
    parser.add_argument(
        "--topic",
        help="Filter by content topic.",
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
                report = build_engagement_velocity_report(
                    conn,
                    window_days=args.window_days,
                    platform=args.platform,
                    topic=args.topic,
                )
        else:
            with script_context() as (_config, db):
                report = build_engagement_velocity_report(
                    db,
                    window_days=args.window_days,
                    platform=args.platform,
                    topic=args.topic,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        print(format_engagement_velocity_csv(report))
    else:
        print(format_engagement_velocity_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
