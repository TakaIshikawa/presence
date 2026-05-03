#!/usr/bin/env python3
"""Audit proactive engagement cooldown frequency by target."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.proactive_cooldown_audit import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MAX_ACTIONS,
    build_proactive_cooldown_audit,
    format_proactive_cooldown_audit_json,
    format_proactive_cooldown_audit_text,
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
        help=f"Lookback window in days by action timestamp (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--max-actions",
        type=_positive_int,
        default=DEFAULT_MAX_ACTIONS,
        help=f"Maximum proactive actions per target in the window (default: {DEFAULT_MAX_ACTIONS}).",
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
                report = build_proactive_cooldown_audit(
                    conn,
                    days=args.days,
                    max_actions=args.max_actions,
                )
        else:
            with script_context() as (_config, db):
                report = build_proactive_cooldown_audit(
                    db,
                    days=args.days,
                    max_actions=args.max_actions,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_proactive_cooldown_audit_json(report))
    else:
        print(format_proactive_cooldown_audit_text(report))
    return 1 if report.blocking_issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
