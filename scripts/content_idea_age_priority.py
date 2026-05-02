#!/usr/bin/env python3
"""Report stale unpublished content ideas by rescue priority."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.content_idea_age_priority import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STALE_DAYS,
    DEFAULT_STATUS,
    STATUSES,
    build_content_idea_age_priority_report,
    build_content_idea_age_priority_report_from_fixture,
    format_content_idea_age_priority_json,
    format_content_idea_age_priority_text,
)


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
        "--stale-days",
        type=_positive_int,
        default=DEFAULT_STALE_DAYS,
        help=f"Minimum age for stale ideas (default: {DEFAULT_STALE_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum ideas per section (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--status",
        choices=(*STATUSES, "all"),
        default=DEFAULT_STATUS,
        help=f"Content idea status to include, or 'all' (default: {DEFAULT_STATUS}).",
    )
    parser.add_argument(
        "--fixture",
        type=Path,
        help="Read content idea records from fixture JSON instead of the database.",
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
    status = None if args.status == "all" else args.status
    try:
        if args.fixture:
            report = build_content_idea_age_priority_report_from_fixture(
                args.fixture,
                stale_days=args.stale_days,
                limit=args.limit,
                status=status,
            )
        else:
            with script_context() as (_config, db):
                report = build_content_idea_age_priority_report(
                    db,
                    stale_days=args.stale_days,
                    limit=args.limit,
                    status=status,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_idea_age_priority_json(report))
    else:
        print(format_content_idea_age_priority_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
