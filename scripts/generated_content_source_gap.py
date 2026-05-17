#!/usr/bin/env python3
"""Report generated content source evidence gaps."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.generated_content_source_gap import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_generated_content_source_gap_report_from_db,
    format_generated_content_source_gap_json,
    format_generated_content_source_gap_text,
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
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--content-type")
    parser.add_argument("--status")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_generated_content_source_gap_report_from_db(
                db, days=args.days, limit=args.limit, content_type=args.content_type, status=args.status
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_generated_content_source_gap_text(report)
        if args.table or args.format == "text"
        else format_generated_content_source_gap_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
