#!/usr/bin/env python3
"""Seed content ideas from recent hotfix-like GitHub commits."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.hotfix_commit_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_SCORE,
    format_hotfix_commit_idea_seed_json,
    format_hotfix_commit_idea_seed_text,
    seed_hotfix_commit_ideas,
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
        help=f"Lookback window in days for commits (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum commit ideas to create or render (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show ideas that would be created without writing to the database.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Render deterministic JSON instead of text.",
    )
    parser.add_argument(
        "--min-score",
        type=_non_negative_int,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum deterministic signal score to include (default: {DEFAULT_MIN_SCORE}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        with script_context() as (_config, db):
            report = seed_hotfix_commit_ideas(
                db,
                days=args.days,
                limit=args.limit,
                min_score=args.min_score,
                dry_run=args.dry_run,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_hotfix_commit_idea_seed_json(report))
    else:
        print(format_hotfix_commit_idea_seed_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
