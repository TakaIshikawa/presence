#!/usr/bin/env python3
"""Plan curated source recrawls from source health and freshness."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_recrawl_backlog import (  # noqa: E402
    DEFAULT_FAILURE_BACKOFF_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_STALE_DAYS,
    build_source_recrawl_backlog_report,
    format_source_recrawl_backlog_json,
    format_source_recrawl_backlog_text,
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
        "--db",
        help="SQLite database path. Defaults to configured database.",
    )
    parser.add_argument(
        "--stale-days",
        type=_positive_int,
        default=DEFAULT_STALE_DAYS,
        help=f"Recrawl sources whose latest successful freshness is this old (default: {DEFAULT_STALE_DAYS}).",
    )
    parser.add_argument(
        "--failure-backoff-days",
        type=_positive_int,
        default=DEFAULT_FAILURE_BACKOFF_DAYS,
        help=(
            "Defer sources with repeated recent failures for this many days "
            f"(default: {DEFAULT_FAILURE_BACKOFF_DAYS})."
        ),
    )
    parser.add_argument(
        "--source-type",
        help="Only include one curated source type, such as x_account, blog, or newsletter.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum rows to output (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_source_recrawl_backlog_report(
                    conn,
                    stale_days=args.stale_days,
                    failure_backoff_days=args.failure_backoff_days,
                    source_type=args.source_type,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_source_recrawl_backlog_report(
                    db,
                    stale_days=args.stale_days,
                    failure_backoff_days=args.failure_backoff_days,
                    source_type=args.source_type,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_source_recrawl_backlog_json(report))
    else:
        print(format_source_recrawl_backlog_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
