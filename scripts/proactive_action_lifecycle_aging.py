#!/usr/bin/env python3
"""Report lifecycle aging for proactive actions."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.proactive_action_lifecycle_aging import (  # noqa: E402
    DEFAULT_APPROVED_NOT_POSTED_HOURS,
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_LOW_RELEVANCE_PERCENT,
    DEFAULT_STALE_PENDING_HOURS,
    build_proactive_action_lifecycle_aging_report,
    format_proactive_action_lifecycle_aging_json,
    format_proactive_action_lifecycle_aging_text,
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
    parser.add_argument(
        "--stale-pending-hours",
        type=_positive_int,
        default=DEFAULT_STALE_PENDING_HOURS,
    )
    parser.add_argument(
        "--approved-not-posted-hours",
        type=_positive_int,
        default=DEFAULT_APPROVED_NOT_POSTED_HOURS,
    )
    parser.add_argument(
        "--low-relevance-percent",
        type=_positive_int,
        default=DEFAULT_LOW_RELEVANCE_PERCENT,
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
            report = build_proactive_action_lifecycle_aging_report(
                db,
                days=args.days,
                limit=args.limit,
                stale_pending_hours=args.stale_pending_hours,
                approved_not_posted_hours=args.approved_not_posted_hours,
                low_relevance_percent=args.low_relevance_percent,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_proactive_action_lifecycle_aging_json(report))
    else:
        print(format_proactive_action_lifecycle_aging_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
