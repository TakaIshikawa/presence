#!/usr/bin/env python3
"""Report high-value knowledge sources that have not been cited."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.knowledge_high_value_uncited_sources import (  # noqa: E402
    DEFAULT_BOOKMARKS_THRESHOLD,
    DEFAULT_CLICKS_THRESHOLD,
    DEFAULT_LIKES_THRESHOLD,
    DEFAULT_LIMIT,
    DEFAULT_RECENT_CURATED_DAYS,
    DEFAULT_REPOSTS_THRESHOLD,
    build_knowledge_high_value_uncited_sources_report_from_db,
    format_knowledge_high_value_uncited_sources_json,
    format_knowledge_high_value_uncited_sources_text,
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


def _non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--likes-threshold", type=_non_negative_float, default=DEFAULT_LIKES_THRESHOLD)
    parser.add_argument("--reposts-threshold", type=_non_negative_float, default=DEFAULT_REPOSTS_THRESHOLD)
    parser.add_argument("--bookmarks-threshold", type=_non_negative_float, default=DEFAULT_BOOKMARKS_THRESHOLD)
    parser.add_argument("--clicks-threshold", type=_non_negative_float, default=DEFAULT_CLICKS_THRESHOLD)
    parser.add_argument("--recent-curated-days", type=_positive_int, default=DEFAULT_RECENT_CURATED_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_knowledge_high_value_uncited_sources_report_from_db(
                db,
                likes_threshold=args.likes_threshold,
                reposts_threshold=args.reposts_threshold,
                bookmarks_threshold=args.bookmarks_threshold,
                clicks_threshold=args.clicks_threshold,
                recent_curated_days=args.recent_curated_days,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_knowledge_high_value_uncited_sources_text(report)
        if args.table or args.format == "text"
        else format_knowledge_high_value_uncited_sources_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
