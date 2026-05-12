#!/usr/bin/env python3
"""Report prompt-maintenance candidates from content feedback."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_feedback_prompt_candidates import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_COUNT,
    build_content_feedback_prompt_candidates_report,
    format_content_feedback_prompt_candidates_json,
    format_content_feedback_prompt_candidates_text,
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--min-count", type=_positive_int, default=DEFAULT_MIN_COUNT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_content_feedback_prompt_candidates_report(
                db, days=args.days, limit=args.limit, min_count=args.min_count
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_content_feedback_prompt_candidates_json(report)
        if args.format == "json"
        else format_content_feedback_prompt_candidates_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
