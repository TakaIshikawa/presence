#!/usr/bin/env python3
"""Report reply draft source freshness by examining knowledge context age."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_source_freshness import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_STALE_DAYS,
    build_reply_source_freshness_report,
    format_reply_source_freshness_json,
    format_reply_source_freshness_text,
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
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for reply drafts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--stale-days",
        type=_positive_int,
        default=DEFAULT_STALE_DAYS,
        help=f"Age threshold in days for stale context (default: {DEFAULT_STALE_DAYS}).",
    )
    parser.add_argument(
        "--status",
        action="append",
        default=None,
        help="Reply status to include (default: pending). Can be repeated.",
    )
    parser.add_argument(
        "--platform",
        action="append",
        default=None,
        help="Platform to filter by (e.g., x, bluesky). Can be repeated.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
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
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_source_freshness_report(
                    conn,
                    days=args.days,
                    stale_days=args.stale_days,
                    status=args.status or ("pending",),
                    platform=args.platform,
                )
        else:
            with script_context() as (_config, db):
                report = build_reply_source_freshness_report(
                    db,
                    days=args.days,
                    stale_days=args.stale_days,
                    status=args.status or ("pending",),
                    platform=args.platform,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "text":
        print(format_reply_source_freshness_text(report))
    else:
        print(format_reply_source_freshness_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
