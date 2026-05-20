#!/usr/bin/env python3
"""Report content idea aging and conversion dropoff."""

from __future__ import annotations

import argparse
from datetime import datetime
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_idea_conversion_dropoff import (  # noqa: E402
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_STALE_DAYS,
    build_content_idea_conversion_dropoff_report_from_db,
    format_content_idea_conversion_dropoff_json,
    format_content_idea_conversion_dropoff_text,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be an ISO datetime") from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--stale-days", type=_non_negative_int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--priority")
    parser.add_argument("--source")
    parser.add_argument("--now", type=_datetime)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"stale_days": args.stale_days, "lookback_days": args.lookback_days, "priority": args.priority, "source": args.source, "now": args.now}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_content_idea_conversion_dropoff_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_content_idea_conversion_dropoff_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_content_idea_conversion_dropoff_text(report) if args.format == "text" else format_content_idea_conversion_dropoff_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
