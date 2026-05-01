#!/usr/bin/env python3
"""Classify recent GitHub activity by synthesis impact."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.github_activity_classifier import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_IMPACT,
    build_github_activity_classification_report,
    format_github_activity_classification_json,
    format_github_activity_classification_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Only include activity updated in the last N days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument("--repo", help="Only include activity from this repo_name.")
    parser.add_argument(
        "--min-impact",
        type=int,
        default=DEFAULT_MIN_IMPACT,
        help=f"Only include classifications with at least this impact score (default: {DEFAULT_MIN_IMPACT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_github_activity_classification_report(
                    conn,
                    days=args.days,
                    repo=args.repo,
                    min_impact=args.min_impact,
                )
        else:
            with script_context() as (_config, db):
                report = build_github_activity_classification_report(
                    db,
                    days=args.days,
                    repo=args.repo,
                    min_impact=args.min_impact,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_github_activity_classification_json(report))
    else:
        print(format_github_activity_classification_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
