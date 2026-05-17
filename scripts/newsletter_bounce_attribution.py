#!/usr/bin/env python3
"""Report newsletter bounce and failed-delivery attribution."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_bounce_attribution import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_newsletter_bounce_attribution_report_from_db,
    format_newsletter_bounce_attribution_json,
    format_newsletter_bounce_attribution_text,
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
    parser.add_argument("--db", help="SQLite database path. Defaults to the configured application database.")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_newsletter_bounce_attribution_report_from_db(conn, days=args.days, limit=args.limit)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_bounce_attribution_report_from_db(db, days=args.days, limit=args.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_newsletter_bounce_attribution_text(report)
        if args.format == "text"
        else format_newsletter_bounce_attribution_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
