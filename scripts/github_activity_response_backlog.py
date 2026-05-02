#!/usr/bin/env python3
"""Report GitHub activity that may need response or content follow-up."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.github_activity_response_backlog import (  # noqa: E402
    ACTIVITY_TYPE_ALL,
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    SUPPORTED_ACTIVITY_TYPES,
    build_github_activity_response_backlog_report,
    format_github_activity_response_backlog_json,
    format_github_activity_response_backlog_text,
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
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum backlog items to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)
        with script_context() as (_config, db):
            report = build_github_activity_response_backlog_report(
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
        print(format_github_activity_response_backlog_json(report))
    else:
        print(format_github_activity_response_backlog_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
