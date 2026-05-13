#!/usr/bin/env python3
"""Report stale supporting context on queued reply drafts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_draft_context_staleness import (  # noqa: E402
    DEFAULT_CONTEXT_MAX_AGE,
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_reply_draft_context_staleness_report,
    format_reply_draft_context_staleness_json,
    format_reply_draft_context_staleness_text,
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
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--context-max-age", type=_positive_int, default=DEFAULT_CONTEXT_MAX_AGE)
    parser.add_argument("--json", action="store_true", help="Output stable JSON instead of text.")
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
                report = build_reply_draft_context_staleness_report(
                    conn,
                    days=args.days,
                    limit=args.limit,
                    context_max_age=args.context_max_age,
                )
        else:
            with script_context() as (_config, db):
                report = build_reply_draft_context_staleness_report(
                    db,
                    days=args.days,
                    limit=args.limit,
                    context_max_age=args.context_max_age,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_reply_draft_context_staleness_json(report)
        if args.json
        else format_reply_draft_context_staleness_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
