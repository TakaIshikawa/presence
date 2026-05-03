#!/usr/bin/env python3
"""Emit GitHub commit synthesis coverage by repository and day."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.github_commit_synthesis_coverage import (  # noqa: E402
    build_github_commit_synthesis_coverage_report,
    format_github_commit_synthesis_coverage_csv,
    format_github_commit_synthesis_coverage_json,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--start-date", help="Only include commits on or after YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Only include commits on or before YYYY-MM-DD.")
    parser.add_argument("--repo", help="Only include commits from this repo_name.")
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (default: json).",
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
                report = build_github_commit_synthesis_coverage_report(
                    conn,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    repo=args.repo,
                )
        else:
            with script_context() as (_config, db):
                report = build_github_commit_synthesis_coverage_report(
                    db,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    repo=args.repo,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        print(format_github_commit_synthesis_coverage_csv(report))
    else:
        print(format_github_commit_synthesis_coverage_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
