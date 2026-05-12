#!/usr/bin/env python3
"""Rank pending drafted replies for review."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_review_priority import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_reply_review_priority_report,
    format_reply_review_priority_json,
    format_reply_review_priority_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--include-low-quality", action="store_true")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_reply_review_priority_report(
                db, days=args.days, limit=args.limit, include_low_quality=args.include_low_quality
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_reply_review_priority_json(report) if args.format == "json" else format_reply_review_priority_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
