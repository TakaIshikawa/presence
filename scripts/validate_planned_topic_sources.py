#!/usr/bin/env python3
"""Validate planned topic source_material references."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.planned_topic_source_validator import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STATUS,
    build_planned_topic_source_validator_report,
    format_planned_topic_source_validator_json,
    format_planned_topic_source_validator_text,
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
        "--campaign-id",
        type=_positive_int,
        help="Campaign id to audit. Defaults to all active/planned campaigns.",
    )
    parser.add_argument(
        "--status",
        default=DEFAULT_STATUS,
        help=f"Planned topic status to audit, or 'all' for any status (default: {DEFAULT_STATUS}).",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum planned topics to audit (default: {DEFAULT_LIMIT}).",
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
            report = build_planned_topic_source_validator_report(
                db,
                campaign_id=args.campaign_id,
                status=args.status,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_planned_topic_source_validator_json(report))
    else:
        print(format_planned_topic_source_validator_text(report))
    return 1 if report.blocking_issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
