#!/usr/bin/env python3
"""Report blog/newsletter cross-link coverage gaps."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_newsletter_crosslink_coverage import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_SHARED_TOKENS,
    DEFAULT_WINDOW_DAYS,
    build_blog_newsletter_crosslink_coverage_report_from_db,
    format_blog_newsletter_crosslink_coverage_json,
    format_blog_newsletter_crosslink_coverage_text,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_int(value: str) -> int:
    parsed = _non_negative_int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-days", type=_non_negative_int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--min-shared-tokens", type=_positive_int, default=DEFAULT_MIN_SHARED_TOKENS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Print the human-readable table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_blog_newsletter_crosslink_coverage_report_from_db(
                db,
                window_days=args.window_days,
                min_shared_tokens=args.min_shared_tokens,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    as_text = args.table or args.format == "text"
    print(
        format_blog_newsletter_crosslink_coverage_text(report)
        if as_text
        else format_blog_newsletter_crosslink_coverage_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
