#!/usr/bin/env python3
"""Report newsletter deliverability trends."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_deliverability_trends import (  # noqa: E402
    DEFAULT_BOUNCE_THRESHOLD,
    DEFAULT_COMPLAINT_THRESHOLD,
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_DAYS,
    build_newsletter_deliverability_trends_report_from_db,
    format_newsletter_deliverability_trends_json,
    format_newsletter_deliverability_trends_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--bounce-threshold", type=_nonnegative_float, default=DEFAULT_BOUNCE_THRESHOLD)
    parser.add_argument("--complaint-threshold", type=_nonnegative_float, default=DEFAULT_COMPLAINT_THRESHOLD)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {
        "lookback_days": args.lookback_days,
        "bounce_threshold": args.bounce_threshold,
        "complaint_threshold": args.complaint_threshold,
        "limit": args.limit,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_newsletter_deliverability_trends_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_deliverability_trends_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_newsletter_deliverability_trends_text(report) if args.format == "text" else format_newsletter_deliverability_trends_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
