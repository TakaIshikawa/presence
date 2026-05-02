#!/usr/bin/env python3
"""Seed content ideas from high-signal GitHub PR review activity."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.github_pr_review_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_SCORE,
    format_github_pr_review_seed_json,
    format_github_pr_review_seed_text,
    seed_github_pr_review_ideas,
)


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
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for PR review activity (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-score",
        type=_non_negative_int,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum deterministic score to seed (default: {DEFAULT_MIN_SCORE}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum recent review rows to inspect (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Return proposed ideas without inserting content_ideas rows.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            results = seed_github_pr_review_ideas(
                db,
                days=args.days,
                min_score=args.min_score,
                limit=args.limit,
                dry_run=args.dry_run,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_github_pr_review_seed_json(results)
        if args.json
        else format_github_pr_review_seed_text(results)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
