#!/usr/bin/env python3
"""Report curated author/account overexposure."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.curated_author_overexposure import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_ITEMS,
    DEFAULT_SHARE_THRESHOLD,
    build_curated_author_overexposure_report_from_db,
    format_curated_author_overexposure_json,
    format_curated_author_overexposure_text,
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


def _share(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--share-threshold", type=_share, default=DEFAULT_SHARE_THRESHOLD)
    parser.add_argument("--min-items", type=_positive_int, default=DEFAULT_MIN_ITEMS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_curated_author_overexposure_report_from_db(
                    conn,
                    days=args.days,
                    share_threshold=args.share_threshold,
                    min_items=args.min_items,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_curated_author_overexposure_report_from_db(
                    db,
                    days=args.days,
                    share_threshold=args.share_threshold,
                    min_items=args.min_items,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_curated_author_overexposure_text(report)
        if args.format == "text"
        else format_curated_author_overexposure_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
