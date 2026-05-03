#!/usr/bin/env python3
"""Audit pending reply drafts for untracked follow-up promises."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_followup_promise_audit import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_reply_followup_promise_audit,
    format_reply_followup_promise_audit_json,
    format_reply_followup_promise_audit_text,
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
        help=f"Lookback window in days for pending reply drafts (default: {DEFAULT_DAYS}).",
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
        help=f"Maximum pending reply drafts to audit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        kwargs = {
            "days": args.days,
            "platform": tuple(args.platform or ()),
            "limit": args.limit,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_followup_promise_audit(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_reply_followup_promise_audit(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_followup_promise_audit_json(report))
    else:
        print(format_reply_followup_promise_audit_text(report))
    if report.blocking_issue_count:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
