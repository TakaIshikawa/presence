#!/usr/bin/env python3
"""Report reply draft relationship and conversation context coverage."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_context_coverage import (  # noqa: E402
    DEFAULT_STATUS,
    build_reply_context_coverage_report,
    format_reply_context_coverage_csv,
    format_reply_context_coverage_json,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--start", help="Inclusive detected_at lower bound.")
    parser.add_argument("--end", help="Inclusive detected_at upper bound.")
    parser.add_argument(
        "--status",
        action="append",
        help=(
            "Reply status to include. Repeat for multiple statuses. "
            f"Defaults to: {', '.join(DEFAULT_STATUS)}."
        ),
    )
    parser.add_argument(
        "--platform",
        action="append",
        help="Platform to include. Repeat for multiple platforms. Defaults to all platforms.",
    )
    parser.add_argument(
        "--account",
        help="Account/platform post filter, applied when a matching column exists.",
    )
    parser.add_argument(
        "--author",
        help="Inbound author handle or ID filter, applied when matching columns exist.",
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
        kwargs = {
            "start": args.start,
            "end": args.end,
            "status": tuple(args.status or DEFAULT_STATUS),
            "platform": tuple(args.platform or ()),
            "account": args.account,
            "author": args.author,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_context_coverage_report(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_reply_context_coverage_report(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        print(format_reply_context_coverage_csv(report))
    else:
        print(format_reply_context_coverage_json(report))
    return 1 if report.blocking_issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
