#!/usr/bin/env python3
"""Report blog draft alt text quality findings."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_draft_alt_text_quality import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_CHARS,
    build_blog_draft_alt_text_quality_report_from_db,
    format_blog_draft_alt_text_quality_json,
    format_blog_draft_alt_text_quality_text,
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--min-chars", type=_positive_int, default=DEFAULT_MIN_CHARS)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {"limit": args.limit, "min_chars": args.min_chars}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_blog_draft_alt_text_quality_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_blog_draft_alt_text_quality_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_blog_draft_alt_text_quality_text(report)
        if args.format == "text"
        else format_blog_draft_alt_text_quality_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
