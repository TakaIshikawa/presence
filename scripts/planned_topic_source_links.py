#!/usr/bin/env python3
"""Audit planned topics for missing generated-content source links."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.planned_topic_source_links import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    build_planned_topic_source_link_report,
    format_planned_topic_source_link_json,
    format_planned_topic_source_link_text,
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
    parser.add_argument(
        "--campaign",
        help="Campaign id, name, slug, or slugified name to audit. Defaults to all campaigns.",
    )
    parser.add_argument(
        "--days-ahead",
        type=_non_negative_int,
        default=DEFAULT_DAYS_AHEAD,
        help=f"Include target dates up to this many days ahead (default: {DEFAULT_DAYS_AHEAD}).",
    )
    parser.add_argument(
        "--include-future",
        action="store_true",
        help="Include all future generated planned topics instead of applying --days-ahead.",
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
            report = build_planned_topic_source_link_report(
                db,
                campaign=args.campaign,
                days_ahead=args.days_ahead,
                include_future=args.include_future,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_planned_topic_source_link_json(report))
    else:
        print(format_planned_topic_source_link_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
