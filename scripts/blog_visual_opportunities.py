#!/usr/bin/env python3
"""Plan blog visual asset opportunities without mutating state."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_visual_opportunities import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_blog_visual_opportunity_report,
    format_blog_visual_opportunity_json,
    format_blog_visual_opportunity_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Only include blog content from the last N days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum opportunities to return (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_blog_visual_opportunity_report(
                    conn,
                    days=args.days,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_blog_visual_opportunity_report(
                    db,
                    days=args.days,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_blog_visual_opportunity_json(report))
    else:
        print(format_blog_visual_opportunity_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
