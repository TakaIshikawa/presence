#!/usr/bin/env python3
"""Find generated content with contradictory durable feedback signals."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_feedback_contradictions import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_content_feedback_contradictions_report,
    format_content_feedback_contradictions_json,
    format_content_feedback_contradictions_text,
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
    parser.add_argument("--tag", help="Only include contradictions involving this normalized tag.")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_content_feedback_contradictions_report(
                db,
                days=args.days,
                limit=args.limit,
                tag=args.tag,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_feedback_contradictions_json(report))
    else:
        print(format_content_feedback_contradictions_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
