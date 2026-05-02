#!/usr/bin/env python3
"""Plan stale generated draft cleanup without mutating the database."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.stale_draft_cleanup import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_EVAL_SCORE,
    build_stale_draft_cleanup_plan,
    format_stale_draft_cleanup_json,
    format_stale_draft_cleanup_text,
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


def _non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Draft age threshold in days by created_at (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum stale drafts to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--min-eval-score",
        type=_non_negative_float,
        default=DEFAULT_MIN_EVAL_SCORE,
        help=(
            "Treat unpublished drafts below this eval_score as failed gate "
            f"(default: {DEFAULT_MIN_EVAL_SCORE})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_stale_draft_cleanup_plan(
                db,
                days=args.days,
                limit=args.limit,
                min_eval_score=args.min_eval_score,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_stale_draft_cleanup_json(report))
    else:
        print(format_stale_draft_cleanup_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
