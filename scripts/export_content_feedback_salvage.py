#!/usr/bin/env python3
"""Export rejected and revised generated content as salvage idea seeds."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.content_feedback_salvage import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    VALID_FEEDBACK_TYPES,
    build_content_feedback_salvage_exports,
    format_content_feedback_salvage_json,
    format_content_feedback_salvage_text,
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
        help=f"Lookback window in days for content feedback (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--feedback-type",
        choices=sorted(VALID_FEEDBACK_TYPES),
        default="all",
        help="Feedback type to include (default: all).",
    )
    parser.add_argument(
        "--content-type",
        default=None,
        help="Only include generated content of this content_type.",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum exports to print; 0 means no limit (default: {DEFAULT_LIMIT}).",
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
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        with script_context() as (_config, db):
            exports = build_content_feedback_salvage_exports(
                db,
                days=args.days,
                feedback_type=args.feedback_type,
                content_type=args.content_type,
                limit=None if args.limit == 0 else args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_feedback_salvage_json(exports))
    else:
        print(format_content_feedback_salvage_text(exports))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
