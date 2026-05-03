#!/usr/bin/env python3
"""Emit JSON age buckets for open publish queue items."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_queue_age_buckets import (  # noqa: E402
    DEFAULT_BUCKET_HOURS,
    DEFAULT_STALE_THRESHOLD_HOURS,
    build_publish_queue_age_bucket_report,
    format_publish_queue_age_bucket_json,
)
from runner import script_context  # noqa: E402


def _non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_float(value: str) -> float:
    parsed = _non_negative_float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--bucket-hours",
        action="append",
        type=_positive_float,
        help=(
            "Age bucket boundary in hours. Repeat for multiple boundaries "
            f"(default: {', '.join(f'{value:g}' for value in DEFAULT_BUCKET_HOURS)})."
        ),
    )
    parser.add_argument(
        "--stale-threshold-hours",
        type=_non_negative_float,
        default=DEFAULT_STALE_THRESHOLD_HOURS,
        help=(
            "Flag open queue items at or above this age in hours "
            f"(default: {DEFAULT_STALE_THRESHOLD_HOURS:g})."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        kwargs = {
            "bucket_hours": args.bucket_hours or DEFAULT_BUCKET_HOURS,
            "stale_threshold_hours": args.stale_threshold_hours,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publish_queue_age_bucket_report(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_publish_queue_age_bucket_report(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_publish_queue_age_bucket_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
