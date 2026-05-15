#!/usr/bin/env python3
"""Report generation review rejection reasons."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.generation_review_rejection_reasons import (  # noqa: E402
    DEFAULT_EXAMPLES_LIMIT,
    DEFAULT_LIMIT,
    build_generation_review_rejection_reasons_report_from_db,
    format_generation_review_rejection_reasons_json,
    format_generation_review_rejection_reasons_text,
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
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--examples-limit", type=_non_negative_int, default=DEFAULT_EXAMPLES_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Print the human-readable table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_generation_review_rejection_reasons_report_from_db(
                db,
                limit=args.limit,
                examples_limit=args.examples_limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    as_text = args.table or args.format == "text"
    print(
        format_generation_review_rejection_reasons_text(report)
        if as_text
        else format_generation_review_rejection_reasons_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
