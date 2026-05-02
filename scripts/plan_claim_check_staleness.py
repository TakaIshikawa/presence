#!/usr/bin/env python3
"""Plan claim-check refresh work before publishing generated content."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.claim_check_staleness import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_STALE_DAYS,
    build_claim_check_staleness_plan,
    format_claim_check_staleness_json,
    format_claim_check_staleness_text,
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
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Generated content lookback in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--stale-days",
        type=_positive_int,
        default=DEFAULT_STALE_DAYS,
        help=f"Passing claim-check age threshold in days (default: {DEFAULT_STALE_DAYS}).",
    )
    parser.add_argument(
        "--include-published",
        action="store_true",
        help="Include generated content that is already marked published.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_claim_check_staleness_plan(
                    conn,
                    days=args.days,
                    stale_days=args.stale_days,
                    include_published=args.include_published,
                )
        else:
            with script_context() as (_config, db):
                report = build_claim_check_staleness_plan(
                    db,
                    days=args.days,
                    stale_days=args.stale_days,
                    include_published=args.include_published,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_claim_check_staleness_json(report))
    else:
        print(format_claim_check_staleness_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
