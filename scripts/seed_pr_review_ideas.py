#!/usr/bin/env python3
"""Seed content ideas from stored GitHub PR review activity."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.pr_review_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_PRIORITY,
    format_pr_review_idea_results_json,
    format_pr_review_idea_results_table,
    seed_pr_review_ideas,
)


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
        help=f"Lookback window in days for PR review activity (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument("--repo", help="Only include activity for this repo name.")
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum eligible rows to process (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--priority",
        choices=("high", "normal", "low"),
        default=DEFAULT_PRIORITY,
        help=f"Priority for created content ideas (default: {DEFAULT_PRIORITY}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Return candidates without inserting content_ideas rows.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            results = seed_pr_review_ideas(
                db,
                days=args.days,
                repo=args.repo,
                limit=args.limit,
                priority=args.priority,
                dry_run=args.dry_run,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_pr_review_idea_results_json(results)
        if args.json
        else format_pr_review_idea_results_table(results)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
