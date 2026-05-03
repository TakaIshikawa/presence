#!/usr/bin/env python3
"""Resolve planned_topics.source_material references to ingested artifacts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.planned_topic_source_resolver import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_planned_topic_source_resolver_report,
    format_planned_topic_source_resolver_json,
    format_planned_topic_source_resolver_text,
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
        "--campaign-id",
        type=_positive_int,
        help="Restrict to one planned_topics.campaign_id.",
    )
    parser.add_argument("--status", help="Restrict to one planned_topics.status.")
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for planned topic rows (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum planned topic rows to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic JSON instead of text.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_planned_topic_source_resolver_report(
                db,
                campaign_id=args.campaign_id,
                status=args.status,
                days=args.days,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_planned_topic_source_resolver_json(report))
    else:
        print(format_planned_topic_source_resolver_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
