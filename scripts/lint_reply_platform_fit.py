#!/usr/bin/env python3
"""Lint pending reply drafts for platform-specific fit issues."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_platform_fit import (  # noqa: E402
    DEFAULT_STATUS,
    SEVERITY_ERROR,
    build_reply_platform_fit_report,
    format_reply_platform_fit_json,
    format_reply_platform_fit_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--platform", help="Only lint drafts for this platform.")
    parser.add_argument(
        "--status",
        default=DEFAULT_STATUS,
        help=f"Only lint drafts with this status (default: {DEFAULT_STATUS}). Use 'all' for every status.",
    )
    parser.add_argument(
        "--min-severity",
        choices=("info", "warn", "error"),
        default=SEVERITY_ERROR,
        help="Only report findings at or above this severity (default: error).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    status = None if args.status == "all" else args.status
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_platform_fit_report(
                    conn,
                    platform=args.platform,
                    status=status,
                    min_severity=args.min_severity,
                )
        else:
            with script_context() as (_config, db):
                report = build_reply_platform_fit_report(
                    db,
                    platform=args.platform,
                    status=status,
                    min_severity=args.min_severity,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_platform_fit_json(report))
    else:
        print(format_reply_platform_fit_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
