#!/usr/bin/env python3
"""Report platform imbalance in the open publish queue."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_queue_platform_skew import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_SKEW_THRESHOLD,
    build_publish_queue_platform_skew_report,
    format_publish_queue_platform_skew_json,
    format_publish_queue_platform_skew_text,
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


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Scheduling horizon for bucket labels (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--skew-threshold",
        type=_non_negative_int,
        default=DEFAULT_SKEW_THRESHOLD,
        help=(
            "Warn only when platform open-count difference exceeds this value "
            f"(default: {DEFAULT_SKEW_THRESHOLD})."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of a human-readable report.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publish_queue_platform_skew_report(
                    conn,
                    days=args.days,
                    skew_threshold=args.skew_threshold,
                )
        else:
            with script_context() as (_config, db):
                report = build_publish_queue_platform_skew_report(
                    db,
                    days=args.days,
                    skew_threshold=args.skew_threshold,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_publish_queue_platform_skew_json(report))
    else:
        print(format_publish_queue_platform_skew_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
