#!/usr/bin/env python3
"""Seed content ideas from imported LinkedIn comments."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.linkedin_comment_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_REACTIONS,
    format_linkedin_comment_seed_json,
    format_linkedin_comment_seed_text,
    seed_linkedin_comment_ideas,
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
        help=f"Lookback window in days for LinkedIn comments (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--min-reactions",
        type=_non_negative_int,
        default=DEFAULT_MIN_REACTIONS,
        help=f"Minimum comment reactions required (default: {DEFAULT_MIN_REACTIONS})",
    )
    parser.add_argument("--limit", type=_positive_int, default=25, help="Maximum groups to inspect")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview ideas without writing content_ideas",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    with script_context() as (_config, db):
        results = seed_linkedin_comment_ideas(
            db,
            days=args.days,
            min_reactions=args.min_reactions,
            limit=args.limit,
            dry_run=args.dry_run,
            now=now,
        )
    output = (
        format_linkedin_comment_seed_json(results)
        if args.format == "json"
        else format_linkedin_comment_seed_text(results)
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
