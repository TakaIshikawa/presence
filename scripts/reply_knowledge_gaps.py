#!/usr/bin/env python3
"""Report reply drafts that lacked useful knowledge support."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_knowledge_gaps import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_QUALITY,
    DEFAULT_STATUS,
    build_reply_knowledge_gap_report,
    format_reply_knowledge_gap_json,
    format_reply_knowledge_gap_text,
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


def _quality_score(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0 or parsed > 10:
        raise argparse.ArgumentTypeError("value must be between 0 and 10")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--status",
        default=DEFAULT_STATUS,
        help=(
            "Comma-separated reply statuses to inspect, or 'all' "
            f"(default: {DEFAULT_STATUS})."
        ),
    )
    parser.add_argument(
        "--min-quality",
        type=_quality_score,
        default=DEFAULT_MIN_QUALITY,
        help=(
            "Low-quality threshold on the 0-10 evaluator scale "
            f"(default: {DEFAULT_MIN_QUALITY:g})."
        ),
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
        level=logging.WARNING if args.format == "json" else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            report = build_reply_knowledge_gap_report(
                db,
                days=args.days,
                status=args.status,
                min_quality=args.min_quality,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_knowledge_gap_json(report))
    else:
        print(format_reply_knowledge_gap_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
