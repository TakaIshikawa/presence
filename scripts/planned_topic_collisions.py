#!/usr/bin/env python3
"""Report planned topics competing for publishing slots or campaign fit."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.planned_topic_collisions import (  # noqa: E402
    DEFAULT_DAYS,
    build_planned_topic_collision_report,
    format_planned_topic_collisions_json,
    format_planned_topic_collisions_text,
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
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for planned topic rows (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--campaign-id",
        type=_positive_int,
        help="Restrict to one planned_topics.campaign_id.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit with status 1 when planned topic collisions are found.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_planned_topic_collision_report(
                db,
                days=args.days,
                campaign_id=args.campaign_id,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_planned_topic_collisions_json(report))
    else:
        print(format_planned_topic_collisions_text(report))
    if args.fail_on_issues and report.has_issues:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
