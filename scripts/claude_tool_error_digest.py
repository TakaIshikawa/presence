#!/usr/bin/env python3
"""Report repeated Claude Code tool error patterns."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.claude_tool_error_digest import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_THRESHOLD,
    build_claude_tool_error_digest,
    format_claude_tool_error_digest_json,
    format_claude_tool_error_digest_text,
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
        help=f"Lookback window in days for Claude events (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--threshold",
        type=_positive_int,
        default=DEFAULT_THRESHOLD,
        help=f"Minimum repeated failures per group (default: {DEFAULT_THRESHOLD}).",
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
                report = build_claude_tool_error_digest(
                    conn,
                    days=args.days,
                    threshold=args.threshold,
                )
        else:
            with script_context() as (_config, db):
                report = build_claude_tool_error_digest(
                    db,
                    days=args.days,
                    threshold=args.threshold,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_claude_tool_error_digest_json(report))
    else:
        print(format_claude_tool_error_digest_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
