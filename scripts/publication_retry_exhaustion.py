#!/usr/bin/env python3
"""Report publication retry exhaustion status."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_retry_exhaustion import (  # noqa: E402
    DEFAULT_NEARLY_EXHAUSTED_RETRIES,
    DEFAULT_RETRY_LIMIT,
    build_publication_retry_exhaustion_report_from_db,
    format_publication_retry_exhaustion_json,
    format_publication_retry_exhaustion_table,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retry-limit", type=_positive_int, default=DEFAULT_RETRY_LIMIT)
    parser.add_argument("--nearly-exhausted-retries", type=_non_negative_int, default=DEFAULT_NEARLY_EXHAUSTED_RETRIES)
    parser.add_argument("--format", choices=("json", "table"), default="json")
    parser.add_argument("--table", action="store_true", help="Print table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_publication_retry_exhaustion_report_from_db(
                db,
                retry_limit=args.retry_limit,
                nearly_exhausted_retries=args.nearly_exhausted_retries,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_publication_retry_exhaustion_table(report)
        if args.table or args.format == "table"
        else format_publication_retry_exhaustion_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
