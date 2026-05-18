#!/usr/bin/env python3
"""Report blog posts that still need X repurposing."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_to_x_repurposing_opportunities import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_TITLE_TOKEN_OVERLAP,
    DEFAULT_WINDOW_DAYS,
    build_blog_to_x_repurposing_opportunities_report_from_db,
    format_blog_to_x_repurposing_opportunities_json,
    format_blog_to_x_repurposing_opportunities_text,
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
    parser.add_argument("--window-days", type=_positive_int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--min-title-token-overlap", type=_positive_int, default=DEFAULT_MIN_TITLE_TOKEN_OVERLAP)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--require-thread", action="store_true")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Output text table.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_blog_to_x_repurposing_opportunities_report_from_db(
                db,
                window_days=args.window_days,
                min_title_token_overlap=args.min_title_token_overlap,
                limit=args.limit,
                require_thread=args.require_thread,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_blog_to_x_repurposing_opportunities_text(report) if args.table or args.format == "text" else format_blog_to_x_repurposing_opportunities_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
