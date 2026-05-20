#!/usr/bin/env python3
"""Report newsletter link click attribution conflicts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_link_click_conflicts import (  # noqa: E402
    DEFAULT_LIMIT,
    build_newsletter_link_click_conflicts_report_from_db,
    format_newsletter_link_click_conflicts_json,
    format_newsletter_link_click_conflicts_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_newsletter_link_click_conflicts_report_from_db(conn, limit=args.limit)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_link_click_conflicts_report_from_db(db, limit=args.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_newsletter_link_click_conflicts_text(report)
        if args.format == "text"
        else format_newsletter_link_click_conflicts_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
