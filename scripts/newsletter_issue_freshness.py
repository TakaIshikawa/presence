#!/usr/bin/env python3
"""Report source artifact freshness for assembled newsletter issues."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_issue_freshness import (  # noqa: E402
    DEFAULT_STALE_DAYS,
    build_newsletter_issue_freshness_report,
    format_newsletter_issue_freshness_csv,
    format_newsletter_issue_freshness_json,
)
from runner import script_context  # noqa: E402


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
        "--start-date",
        help="Inclusive sent_at lower bound, as YYYY-MM-DD or ISO datetime.",
    )
    parser.add_argument(
        "--end-date",
        help="Inclusive sent_at upper bound, as YYYY-MM-DD or ISO datetime.",
    )
    parser.add_argument(
        "--stale-days",
        type=_non_negative_int,
        default=DEFAULT_STALE_DAYS,
        help=f"Count a section stale when source age exceeds this many days (default: {DEFAULT_STALE_DAYS}).",
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
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)

        with script_context() as (_config, db):
            report = build_newsletter_issue_freshness_report(
                db,
                start_date=args.start_date,
                end_date=args.end_date,
                stale_days=args.stale_days,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        print(format_newsletter_issue_freshness_csv(report))
    else:
        print(format_newsletter_issue_freshness_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
