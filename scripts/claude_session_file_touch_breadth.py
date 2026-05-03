#!/usr/bin/env python3
"""Emit Claude Code session file-touch breadth JSON."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.claude_session_file_touch_breadth import (  # noqa: E402
    DEFAULT_LIMIT,
    build_claude_session_file_touch_breadth_report,
    format_claude_session_file_touch_breadth_json,
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
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum session rows to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--repo-root",
        default=str(Path.cwd()),
        help="Repository root for normalizing absolute paths (default: current directory).",
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
                report = build_claude_session_file_touch_breadth_report(
                    conn,
                    limit=args.limit,
                    repo_root=args.repo_root,
                )
        else:
            with script_context() as (_config, db):
                report = build_claude_session_file_touch_breadth_report(
                    db,
                    limit=args.limit,
                    repo_root=args.repo_root,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_claude_session_file_touch_breadth_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
