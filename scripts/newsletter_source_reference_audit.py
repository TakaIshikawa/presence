#!/usr/bin/env python3
"""Audit newsletter_sends.source_content_ids broken references."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_source_reference_audit import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_newsletter_source_reference_audit_report,
    build_newsletter_source_reference_audit_report_from_fixture,
    format_newsletter_source_reference_audit_json,
    format_newsletter_source_reference_audit_text,
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
        help=f"Recent newsletter send lookback in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum newsletter sends to audit; 0 means no limit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fixture",
        type=Path,
        help="Read newsletter send and generated content rows from fixture JSON.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        limit = None if args.limit == 0 else args.limit
        if args.fixture:
            report = build_newsletter_source_reference_audit_report_from_fixture(
                args.fixture,
                days=args.days,
                limit=limit,
            )
        else:
            with script_context() as (_config, db):
                report = build_newsletter_source_reference_audit_report(
                    db,
                    days=args.days,
                    limit=limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_source_reference_audit_json(report))
    else:
        print(format_newsletter_source_reference_audit_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
