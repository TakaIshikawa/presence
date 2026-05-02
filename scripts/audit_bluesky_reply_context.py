#!/usr/bin/env python3
"""Audit Bluesky reply drafts for missing imported thread context."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.bluesky_reply_context_gaps import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_STATUS,
    audit_bluesky_reply_context_gaps,
    format_bluesky_reply_context_gaps_json,
    format_bluesky_reply_context_gaps_text,
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
        help=f"Lookback window in days for reply drafts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--status",
        default=DEFAULT_STATUS,
        help=f"Reply status to audit, or 'all' for any status (default: {DEFAULT_STATUS}).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic JSON instead of compact text.",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum reply drafts to audit (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = audit_bluesky_reply_context_gaps(
                db,
                days=args.days,
                status=args.status,
                limit=args.limit,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_bluesky_reply_context_gaps_json(report))
    else:
        print(format_bluesky_reply_context_gaps_text(report))
    if report.blocking_issue_count:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
