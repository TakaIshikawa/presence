#!/usr/bin/env python3
"""Seed content ideas from stored GitHub Discussion activity."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.github_discussion_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_BODY_LENGTH,
    format_github_discussion_idea_seed_json,
    format_github_discussion_idea_seed_text,
    seed_github_discussion_ideas,
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
        raise argparse.ArgumentTypeError("value must be zero or positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for discussion activity (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument("--repo", help="Only include activity for this repo name.")
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum eligible candidates to process (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--min-body-length",
        type=_non_negative_int,
        default=DEFAULT_MIN_BODY_LENGTH,
        help=(
            "Minimum normalized body length before an idea is considered "
            f"(default: {DEFAULT_MIN_BODY_LENGTH})."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Return candidates without inserting content_ideas rows.",
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
        with script_context() as (_config, db):
            report = seed_github_discussion_ideas(
                db,
                days=args.days,
                repo=args.repo,
                limit=args.limit,
                min_body_length=args.min_body_length,
                dry_run=args.dry_run,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_github_discussion_idea_seed_json(report))
    else:
        print(format_github_discussion_idea_seed_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
