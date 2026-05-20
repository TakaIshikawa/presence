#!/usr/bin/env python3
"""Report crowded publish queue schedule buckets by platform."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_queue_slot_contention import (  # noqa: E402
    DEFAULT_BUCKET_SIZE_MINUTES,
    DEFAULT_LOOKAHEAD_HOURS,
    DEFAULT_MAX_POSTS_PER_BUCKET,
    build_publish_queue_slot_contention_report_from_db,
    format_publish_queue_slot_contention_json,
    format_publish_queue_slot_contention_text,
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
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument(
        "--bucket-size-minutes",
        type=_positive_int,
        default=DEFAULT_BUCKET_SIZE_MINUTES,
    )
    parser.add_argument(
        "--lookahead-hours",
        type=_positive_int,
        default=DEFAULT_LOOKAHEAD_HOURS,
    )
    parser.add_argument(
        "--max-posts-per-bucket",
        type=_positive_int,
        default=DEFAULT_MAX_POSTS_PER_BUCKET,
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    kwargs = {
        "bucket_size_minutes": args.bucket_size_minutes,
        "lookahead_hours": args.lookahead_hours,
        "max_posts_per_bucket": args.max_posts_per_bucket,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publish_queue_slot_contention_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_publish_queue_slot_contention_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_publish_queue_slot_contention_text(report)
        if args.format == "text"
        else format_publish_queue_slot_contention_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
