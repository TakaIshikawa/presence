#!/usr/bin/env python3
"""Export Claude Code session interruption markers."""

from __future__ import annotations

import argparse
from datetime import datetime
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.claude_session_interruptions import (  # noqa: E402
    export_claude_session_interruptions,
    format_claude_session_interruptions_json,
    format_claude_session_interruptions_markdown,
)


def _since(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise argparse.ArgumentTypeError("value must not be empty")
    try:
        if len(cleaned) == 10:
            datetime.fromisoformat(cleaned)
        else:
            datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date or datetime: {value}") from exc
    return cleaned


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--since",
        type=_since,
        help="Only scan Claude messages at or after this ISO date/datetime.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
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
                records = export_claude_session_interruptions(conn, since=args.since)
        else:
            with script_context() as (_config, db):
                records = export_claude_session_interruptions(db, since=args.since)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "markdown":
        print(format_claude_session_interruptions_markdown(records))
    else:
        print(format_claude_session_interruptions_json(records))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
