#!/usr/bin/env python3
"""Seed content ideas from unresolved Claude Code blockers."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.claude_blocker_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    format_claude_blocker_ideas_json,
    format_claude_blocker_ideas_text,
    seed_claude_blocker_ideas,
)


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
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for Claude messages (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum candidates to process (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report candidates without writing content ideas.",
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
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                results = seed_claude_blocker_ideas(
                    conn,
                    days=args.days,
                    limit=args.limit,
                    dry_run=args.dry_run,
                )
        else:
            with script_context() as (_config, db):
                results = seed_claude_blocker_ideas(
                    db,
                    days=args.days,
                    limit=args.limit,
                    dry_run=args.dry_run,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_claude_blocker_ideas_json(results))
    else:
        print(format_claude_blocker_ideas_text(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
