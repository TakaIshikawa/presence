#!/usr/bin/env python3
"""Report GitHub discussions that have not produced generated content."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.github_discussion_followthrough import (  # noqa: E402
    DEFAULT_DAYS_STALE,
    build_github_discussion_followthrough_report,
    format_github_discussion_followthrough_csv,
    format_github_discussion_followthrough_json,
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
    parser.add_argument(
        "--days-stale",
        type=_positive_int,
        default=DEFAULT_DAYS_STALE,
        help=f"Age in days at which uncovered discussions are stale (default: {DEFAULT_DAYS_STALE}).",
    )
    parser.add_argument("--repo", help="Only include discussions from this repo_name.")
    parser.add_argument("--label", help="Only include discussions with this label.")
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="csv",
        help="Output format (default: csv).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_github_discussion_followthrough_report(
                    conn,
                    days_stale=args.days_stale,
                    repo=args.repo,
                    label=args.label,
                )
        else:
            with script_context() as (_config, db):
                report = build_github_discussion_followthrough_report(
                    db,
                    days_stale=args.days_stale,
                    repo=args.repo,
                    label=args.label,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_github_discussion_followthrough_json(report))
    else:
        print(format_github_discussion_followthrough_csv(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
