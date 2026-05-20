#!/usr/bin/env python3
"""Audit engagement metrics against publication state."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.engagement_metric_publication_gaps import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MINIMUM_AGE_HOURS,
    build_engagement_metric_publication_gaps_report_from_db,
    format_engagement_metric_publication_gaps_json,
    format_engagement_metric_publication_gaps_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--minimum-age-hours", type=_positive_int, default=DEFAULT_MINIMUM_AGE_HOURS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {"minimum_age_hours": args.minimum_age_hours, "limit": args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_engagement_metric_publication_gaps_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_engagement_metric_publication_gaps_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_engagement_metric_publication_gaps_text(report) if args.format == "text" else format_engagement_metric_publication_gaps_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
