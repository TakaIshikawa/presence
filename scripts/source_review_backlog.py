#!/usr/bin/env python3
"""Report discovered curated sources awaiting review."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_review_backlog import (  # noqa: E402
    DEFAULT_DAYS,
    build_source_review_backlog_report,
    format_source_review_backlog_json,
    format_source_review_backlog_text,
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
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Curated source discovery lookback in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--source-type",
        help="Only include one curated source type, such as x_account, blog, or newsletter.",
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
        with script_context() as (_config, db):
            report = build_source_review_backlog_report(
                db,
                days=args.days,
                source_type=args.source_type,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_source_review_backlog_json(report))
    else:
        print(format_source_review_backlog_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
