#!/usr/bin/env python3
"""Report generated blog content publication followthrough issues."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_publication_followthrough import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_AGE_DAYS,
    build_blog_publication_followthrough_report,
    format_blog_publication_followthrough_json,
    format_blog_publication_followthrough_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--min-age-days", type=_nonnegative_int, default=DEFAULT_MIN_AGE_DAYS)
    parser.add_argument("--issue-type")
    parser.add_argument("--format", choices=("text", "json"), default="text")
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
                report = build_blog_publication_followthrough_report(conn, days=args.days, min_age_days=args.min_age_days, issue_type=args.issue_type)
        else:
            with script_context() as (_config, db):
                report = build_blog_publication_followthrough_report(db, days=args.days, min_age_days=args.min_age_days, issue_type=args.issue_type)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_blog_publication_followthrough_json(report) if args.format == "json" else format_blog_publication_followthrough_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
