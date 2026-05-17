#!/usr/bin/env python3
"""Report newsletter subject promise fulfillment."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_subject_promise_fulfillment import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_OVERLAP,
    build_newsletter_subject_promise_fulfillment_report,
    format_newsletter_subject_promise_fulfillment_json,
    format_newsletter_subject_promise_fulfillment_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _ratio(value: str) -> float:
    parsed = float(value)
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--min-overlap", type=_ratio, default=DEFAULT_MIN_OVERLAP)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_newsletter_subject_promise_fulfillment_report(db, days=args.days, min_overlap=args.min_overlap)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_text = args.table or args.format == "text"
    print(format_newsletter_subject_promise_fulfillment_text(report) if as_text else format_newsletter_subject_promise_fulfillment_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
