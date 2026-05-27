#!/usr/bin/env python3
"""Report accessibility gaps in blog draft tables."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_draft_table_accessibility_gaps import (  # noqa: E402
    DEFAULT_LIMIT,
    build_blog_draft_table_accessibility_gaps_report_from_db,
    format_blog_draft_table_accessibility_gaps_json,
    format_blog_draft_table_accessibility_gaps_text,
)
from runner import script_context  # noqa: E402


def _positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--draft-id")
    parser.add_argument("--limit", type=_positive, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"draft_id": args.draft_id, "limit": args.limit}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_blog_draft_table_accessibility_gaps_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_blog_draft_table_accessibility_gaps_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_blog_draft_table_accessibility_gaps_text(report)
        if args.format == "text"
        else format_blog_draft_table_accessibility_gaps_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
