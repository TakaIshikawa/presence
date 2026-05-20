#!/usr/bin/env python3
"""Report missing or stale newsletter engagement fetches."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_engagement_fetch_lag import (  # noqa: E402
    DEFAULT_GRACE_HOURS,
    DEFAULT_LIMIT,
    DEFAULT_STALE_AFTER_HOURS,
    DEFAULT_STATUSES,
    build_newsletter_engagement_fetch_lag_report_from_db,
    format_newsletter_engagement_fetch_lag_json,
    format_newsletter_engagement_fetch_lag_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--grace-hours", type=_non_negative_float, default=DEFAULT_GRACE_HOURS)
    parser.add_argument("--stale-after-hours", type=_positive_float, default=DEFAULT_STALE_AFTER_HOURS)
    parser.add_argument(
        "--status",
        action="append",
        dest="statuses",
        help=f"Newsletter send status to include. Can be repeated. Default: {', '.join(DEFAULT_STATUSES)}.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        kwargs = {
            "grace_hours": args.grace_hours,
            "stale_after_hours": args.stale_after_hours,
            "limit": args.limit,
            "status_filters": tuple(args.statuses or DEFAULT_STATUSES),
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_newsletter_engagement_fetch_lag_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_engagement_fetch_lag_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_newsletter_engagement_fetch_lag_text(report)
        if args.format == "text"
        else format_newsletter_engagement_fetch_lag_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
