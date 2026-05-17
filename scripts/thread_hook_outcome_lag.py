#!/usr/bin/env python3
"""Report thread hook styles with stale outcome feedback."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.thread_hook_outcome_lag import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_POSTS,
    DEFAULT_STALE_AFTER_DAYS,
    build_thread_hook_outcome_lag_report,
    format_thread_hook_outcome_lag_json,
    format_thread_hook_outcome_lag_text,
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
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--stale-after-days", type=_non_negative_int, default=DEFAULT_STALE_AFTER_DAYS)
    parser.add_argument("--min-posts", type=_positive_int, default=DEFAULT_MIN_POSTS)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Print the human-readable table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_thread_hook_outcome_lag_report(
                db,
                days=args.days,
                stale_after_days=args.stale_after_days,
                min_posts=args.min_posts,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_thread_hook_outcome_lag_text(report)
        if args.table or args.format == "text"
        else format_thread_hook_outcome_lag_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
