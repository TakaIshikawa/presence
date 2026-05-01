#!/usr/bin/env python3
"""Plan closing actions for inbound reply conversations."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_conversation_closer import (  # noqa: E402
    DEFAULT_MAX_THREAD_AGE_HOURS,
    DEFAULT_MIN_EXCHANGE_COUNT,
    build_reply_conversation_closer_report,
    format_reply_conversation_closer_json,
    format_reply_conversation_closer_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--max-age-hours",
        type=int,
        default=DEFAULT_MAX_THREAD_AGE_HOURS,
        help=(
            "Maximum age since the latest thread activity before no_action "
            f"(default: {DEFAULT_MAX_THREAD_AGE_HOURS})."
        ),
    )
    parser.add_argument(
        "--min-exchanges",
        type=int,
        default=DEFAULT_MIN_EXCHANGE_COUNT,
        help=(
            "Minimum rows in a thread before repeated back-and-forth is escalated "
            f"(default: {DEFAULT_MIN_EXCHANGE_COUNT})."
        ),
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
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_reply_conversation_closer_report(
                    conn,
                    max_thread_age_hours=args.max_age_hours,
                    min_exchange_count=args.min_exchanges,
                )
        else:
            with script_context() as (_config, db):
                report = build_reply_conversation_closer_report(
                    db,
                    max_thread_age_hours=args.max_age_hours,
                    min_exchange_count=args.min_exchanges,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_conversation_closer_json(report))
    else:
        print(format_reply_conversation_closer_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
