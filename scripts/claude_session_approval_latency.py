#!/usr/bin/env python3
"""Report Claude Code approval request-to-decision latency."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.claude_session_approval_latency import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_SLOW_THRESHOLD_SECONDS,
    build_claude_session_approval_latency_report,
    format_claude_session_approval_latency_json,
    format_claude_session_approval_latency_text,
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
        help=f"Lookback window in days for Claude approval events (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--slow-threshold-seconds",
        type=_positive_int,
        default=DEFAULT_SLOW_THRESHOLD_SECONDS,
        help=(
            "Elapsed seconds at or above which an approval is slow "
            f"(default: {DEFAULT_SLOW_THRESHOLD_SECONDS})."
        ),
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum paired approval rows to emit (default: {DEFAULT_LIMIT}).",
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
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_claude_session_approval_latency_report(
                    conn,
                    days=args.days,
                    slow_threshold_seconds=args.slow_threshold_seconds,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_claude_session_approval_latency_report(
                    db,
                    days=args.days,
                    slow_threshold_seconds=args.slow_threshold_seconds,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_claude_session_approval_latency_json(report))
    else:
        print(format_claude_session_approval_latency_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
