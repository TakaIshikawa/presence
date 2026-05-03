#!/usr/bin/env python3
"""Audit queued reply drafts for duplicate targets or duplicate intent."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_duplicate_intent_audit import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_STATUSES,
    build_reply_duplicate_intent_audit,
    format_reply_duplicate_intent_audit_json,
    format_reply_duplicate_intent_audit_markdown,
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
        help=f"Lookback window in days for queued reply drafts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--status",
        action="append",
        dest="statuses",
        help=(
            "Reply status to audit. Repeat for multiple statuses "
            f"(default: {', '.join(DEFAULT_STATUSES)})."
        ),
    )
    parser.add_argument(
        "--platform",
        action="append",
        help="Platform to audit. Repeat for multiple platforms. Defaults to all platforms.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum duplicate groups to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="Output format (default: markdown).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_reply_duplicate_intent_audit(
                db,
                days=args.days,
                statuses=tuple(args.statuses or ()),
                platform=tuple(args.platform or ()),
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_duplicate_intent_audit_json(report))
    else:
        print(format_reply_duplicate_intent_audit_markdown(report))
    return 1 if report.blocking_issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
