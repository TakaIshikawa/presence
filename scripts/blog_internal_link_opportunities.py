#!/usr/bin/env python3
"""Report blog internal link opportunities."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_internal_link_opportunities import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_SCORE,
    build_blog_internal_link_opportunities_report,
    format_blog_internal_link_opportunities_json,
    format_blog_internal_link_opportunities_text,
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
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--min-score", type=_positive_int, default=DEFAULT_MIN_SCORE)
    parser.add_argument("--json", action="store_true", help="Output stable JSON instead of text.")
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
                report = build_blog_internal_link_opportunities_report(conn, days=args.days, limit=args.limit, min_score=args.min_score)
        else:
            with script_context() as (_config, db):
                report = build_blog_internal_link_opportunities_report(db, days=args.days, limit=args.limit, min_score=args.min_score)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_blog_internal_link_opportunities_json(report) if args.json else format_blog_internal_link_opportunities_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
