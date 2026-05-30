#!/usr/bin/env python3
"""Report planned content topic concentration."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_calendar_topic_concentration import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_TOPIC_ITEMS,
    DEFAULT_WINDOW_DAYS,
    build_content_calendar_topic_concentration_report_from_db,
    format_content_calendar_topic_concentration_json,
    format_content_calendar_topic_concentration_text,
)
from runner import script_context  # noqa: E402


def _pos(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--window-days", type=_pos, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--max-topic-items", type=_pos, default=DEFAULT_MAX_TOPIC_ITEMS)
    parser.add_argument("--required-categories", default="")
    parser.add_argument("--limit", type=_pos, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"window_days": args.window_days, "max_topic_items": args.max_topic_items, "required_categories": args.required_categories, "limit": args.limit}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_content_calendar_topic_concentration_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_content_calendar_topic_concentration_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_content_calendar_topic_concentration_text(report) if args.format == "text" else format_content_calendar_topic_concentration_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
