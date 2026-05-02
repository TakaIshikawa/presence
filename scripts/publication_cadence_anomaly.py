#!/usr/bin/env python3
"""Report unusual cadence across published and queued scheduled posts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publication_cadence_anomaly import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_LOOKAHEAD_DAYS,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MAX_GAP_HOURS,
    DEFAULT_MAX_POSTS_PER_WINDOW,
    DEFAULT_REPEATED_HOUR_THRESHOLD,
    DEFAULT_WINDOW_HOURS,
    build_publication_cadence_anomaly_report,
    format_publication_cadence_anomaly_json,
    format_publication_cadence_anomaly_text,
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


def _positive_int(value: str) -> int:
    parsed = _non_negative_int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--lookback-days",
        type=_non_negative_int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Published-post lookback window in days (default: {DEFAULT_LOOKBACK_DAYS}).",
    )
    parser.add_argument(
        "--lookahead-days",
        type=_non_negative_int,
        default=DEFAULT_LOOKAHEAD_DAYS,
        help=f"Queued schedule lookahead window in days (default: {DEFAULT_LOOKAHEAD_DAYS}).",
    )
    parser.add_argument(
        "--window-hours",
        type=_positive_int,
        default=DEFAULT_WINDOW_HOURS,
        help=f"Burst detection window in hours (default: {DEFAULT_WINDOW_HOURS}).",
    )
    parser.add_argument(
        "--max-posts-per-window",
        type=_positive_int,
        default=DEFAULT_MAX_POSTS_PER_WINDOW,
        help=(
            "Flag burst windows with more than this many posts "
            f"(default: {DEFAULT_MAX_POSTS_PER_WINDOW})."
        ),
    )
    parser.add_argument(
        "--max-gap-hours",
        type=_positive_int,
        default=DEFAULT_MAX_GAP_HOURS,
        help=f"Flag silence gaps longer than this many hours (default: {DEFAULT_MAX_GAP_HOURS}).",
    )
    parser.add_argument(
        "--repeated-hour-threshold",
        type=_positive_int,
        default=DEFAULT_REPEATED_HOUR_THRESHOLD,
        help=(
            "Summarize same-hour patterns at or above this event count "
            f"(default: {DEFAULT_REPEATED_HOUR_THRESHOLD})."
        ),
    )
    parser.add_argument(
        "--platform",
        choices=("all", "x", "bluesky"),
        default="all",
        help="Filter to one effective platform (default: all).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum rows per section (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of a human-readable report.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publication_cadence_anomaly_report(
                    conn,
                    lookback_days=args.lookback_days,
                    lookahead_days=args.lookahead_days,
                    window_hours=args.window_hours,
                    max_posts_per_window=args.max_posts_per_window,
                    max_gap_hours=args.max_gap_hours,
                    repeated_hour_threshold=args.repeated_hour_threshold,
                    platform=args.platform,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_publication_cadence_anomaly_report(
                    db,
                    lookback_days=args.lookback_days,
                    lookahead_days=args.lookahead_days,
                    window_hours=args.window_hours,
                    max_posts_per_window=args.max_posts_per_window,
                    max_gap_hours=args.max_gap_hours,
                    repeated_hour_threshold=args.repeated_hour_threshold,
                    platform=args.platform,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json or args.format == "json":
        print(format_publication_cadence_anomaly_json(report))
    else:
        print(format_publication_cadence_anomaly_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
