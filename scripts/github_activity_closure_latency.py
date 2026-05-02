#!/usr/bin/env python3
"""Report GitHub issue and pull request closure latency."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.github_activity_closure_latency import (  # noqa: E402
    ACTIVITY_TYPE_ALL,
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    SUPPORTED_ACTIVITY_TYPES,
    build_github_activity_closure_latency_report,
    format_github_activity_closure_latency_json,
    format_github_activity_closure_latency_text,
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
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Activity lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument("--repo", help="Only include activity from this repo_name.")
    parser.add_argument(
        "--activity-type",
        choices=(ACTIVITY_TYPE_ALL, *SUPPORTED_ACTIVITY_TYPES),
        default=ACTIVITY_TYPE_ALL,
        help="GitHub activity type to include (default: all).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum stale open items to emit (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_github_activity_closure_latency_report(
                db,
                days=args.days,
                repo=args.repo,
                activity_type=args.activity_type,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_github_activity_closure_latency_json(report))
    else:
        print(format_github_activity_closure_latency_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
