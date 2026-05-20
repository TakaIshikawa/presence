#!/usr/bin/env python3
"""Report pending proactive actions with stale or missing context."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.proactive_action_context_decay import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_CONTEXT_AGE_DAYS,
    build_proactive_action_context_decay_report_from_db,
    format_proactive_action_context_decay_json,
    format_proactive_action_context_decay_text,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--max-context-age-days", type=_non_negative_int, default=DEFAULT_MAX_CONTEXT_AGE_DAYS)
    parser.add_argument("--status", default="pending", help="Comma-separated status filter.")
    parser.add_argument("--action-type", default="all", help="Comma-separated action type filter.")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {
            "max_context_age_days": args.max_context_age_days,
            "status": args.status,
            "action_type": args.action_type,
            "limit": args.limit,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_proactive_action_context_decay_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_proactive_action_context_decay_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_proactive_action_context_decay_text(report) if args.format == "text" else format_proactive_action_context_decay_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
