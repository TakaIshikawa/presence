#!/usr/bin/env python3
"""Report newsletter archive broken links."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_archive_broken_links import (  # noqa: E402
    DEFAULT_BAD_STATUS_CODES,
    DEFAULT_LIMIT,
    build_newsletter_archive_broken_links_report_from_db,
    format_newsletter_archive_broken_links_json,
    format_newsletter_archive_broken_links_text,
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
    parser.add_argument("--bad-status-codes", default=DEFAULT_BAD_STATUS_CODES)
    parser.add_argument("--limit", type=_pos, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_newsletter_archive_broken_links_report_from_db(conn, bad_status_codes=args.bad_status_codes, limit=args.limit)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_archive_broken_links_report_from_db(db, bad_status_codes=args.bad_status_codes, limit=args.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_newsletter_archive_broken_links_text(report) if args.format == "text" else format_newsletter_archive_broken_links_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
