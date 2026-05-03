#!/usr/bin/env python3
"""Report current publication reliability streaks by platform."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_reliability_streaks import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_FAILURE_THRESHOLD,
    build_publication_reliability_streak_report,
    format_publication_reliability_streak_json,
    format_publication_reliability_streak_text,
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
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back for attempts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--failure-threshold",
        type=_positive_int,
        default=DEFAULT_FAILURE_THRESHOLD,
        help=(
            "Consecutive current failures before attention is flagged "
            f"(default: {DEFAULT_FAILURE_THRESHOLD})."
        ),
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
                report = build_publication_reliability_streak_report(
                    conn,
                    days=args.days,
                    failure_threshold=args.failure_threshold,
                )
        else:
            with script_context() as (_config, db):
                report = build_publication_reliability_streak_report(
                    db,
                    days=args.days,
                    failure_threshold=args.failure_threshold,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publication_reliability_streak_json(report))
    else:
        print(format_publication_reliability_streak_text(report))
    return 1 if report.totals["attention_platform_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
