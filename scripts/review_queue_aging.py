#!/usr/bin/env python3
"""Report aging generated content waiting on review or publication."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.review_queue_aging import (  # noqa: E402
    DEFAULT_BUCKET_DAYS,
    DEFAULT_LIMIT,
    build_review_queue_aging_report,
    format_review_queue_aging_json,
    format_review_queue_aging_text,
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


def _bucket_days(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(int(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid bucket days: {value}") from exc
    if not parsed:
        raise argparse.ArgumentTypeError("bucket days must not be empty")
    if any(day <= 0 for day in parsed):
        raise argparse.ArgumentTypeError("bucket days must be positive")
    if len(set(parsed)) != len(parsed):
        raise argparse.ArgumentTypeError("bucket days must be unique")
    return tuple(sorted(parsed))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bucket-days",
        type=_bucket_days,
        default=DEFAULT_BUCKET_DAYS,
        help="Comma-separated ascending age bucket thresholds in days.",
    )
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_review_queue_aging_report(
                db,
                bucket_days=args.bucket_days,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_review_queue_aging_json(report)
        if args.format == "json"
        else format_review_queue_aging_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
