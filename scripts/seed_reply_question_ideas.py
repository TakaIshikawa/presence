#!/usr/bin/env python3
"""Seed content ideas from repeated unanswered reply questions."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_question_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_CLUSTER_SIZE,
    format_reply_question_ideas_json,
    format_reply_question_ideas_text,
    seed_reply_question_ideas,
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
        help=f"Look back at reply questions from the last N days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-cluster-size",
        type=_positive_int,
        default=DEFAULT_MIN_CLUSTER_SIZE,
        help=f"Minimum similar questions required for an idea (default: {DEFAULT_MIN_CLUSTER_SIZE}).",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum clusters to process (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report candidate ideas without writing content_ideas rows.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="text",
        help="Output format (default: text).",
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
            results = seed_reply_question_ideas(
                db,
                days=args.days,
                min_cluster_size=args.min_cluster_size,
                limit=args.limit,
                dry_run=args.dry_run,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_question_ideas_json(results))
    else:
        print(format_reply_question_ideas_text(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
