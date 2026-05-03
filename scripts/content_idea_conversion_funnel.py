#!/usr/bin/env python3
"""Export content idea conversion by source type and funnel stage."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.content_idea_conversion_funnel import (  # noqa: E402
    DEFAULT_MIN_AGE_DAYS,
    build_content_idea_conversion_funnel_report,
    format_content_idea_conversion_funnel_csv,
    format_content_idea_conversion_funnel_json,
)


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
    parser.add_argument("--start-date", help="Include ideas created on or after this ISO date.")
    parser.add_argument("--end-date", help="Include ideas created on or before this ISO date.")
    parser.add_argument("--source-type", help="Restrict to one content_ideas.source value.")
    parser.add_argument(
        "--min-age-days",
        type=_non_negative_int,
        default=DEFAULT_MIN_AGE_DAYS,
        help=f"Age threshold for stale ideas (default: {DEFAULT_MIN_AGE_DAYS}).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (default: json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_content_idea_conversion_funnel_report(
                db,
                start_date=args.start_date,
                end_date=args.end_date,
                source_type=args.source_type,
                min_age_days=args.min_age_days,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        print(format_content_idea_conversion_funnel_csv(report))
    else:
        print(format_content_idea_conversion_funnel_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
