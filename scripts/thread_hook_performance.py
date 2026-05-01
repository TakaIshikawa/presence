#!/usr/bin/env python3
"""Report which X thread opening styles perform best."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.thread_hook_performance import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_COUNT,
    build_thread_hook_performance_report,
    format_thread_hook_performance_json,
    format_thread_hook_performance_table,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=DEFAULT_MIN_COUNT,
        help=f"Minimum samples per hook style (default: {DEFAULT_MIN_COUNT})",
    )
    parser.add_argument(
        "--examples",
        type=int,
        default=0,
        help="Number of top examples to include per style",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        rows = build_thread_hook_performance_report(
            db,
            days=args.days,
            min_count=args.min_count,
            examples=args.examples,
        )

    output = (
        format_thread_hook_performance_json(rows)
        if args.json
        else format_thread_hook_performance_table(
            rows,
            days=args.days,
            min_count=args.min_count,
        )
    )
    print(output)


if __name__ == "__main__":
    main()
