#!/usr/bin/env python3
"""Report Claude session linkage coverage for recent GitHub commits."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.claude_work_coverage import (  # noqa: E402
    build_claude_work_coverage_report,
    format_claude_work_coverage_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Lookback window in days (default: 14)",
    )
    parser.add_argument(
        "--repo",
        help="Restrict commits to one github_commits.repo_name",
    )
    parser.add_argument(
        "--min-commits",
        type=int,
        default=3,
        help="Minimum daily commits before an unlinked day is flagged (default: 3)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum unlinked commits/messages to include (default: 10)",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_claude_work_coverage_report(
                db,
                days=args.days,
                repo=args.repo,
                min_commits=args.min_commits,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(format_claude_work_coverage_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
