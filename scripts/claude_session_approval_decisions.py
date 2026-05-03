#!/usr/bin/env python3
"""Report Claude Code approval decisions followed by session activity."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.claude_session_approval_decision_audit import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_WINDOW_SIZE,
    build_claude_session_approval_decision_audit_report,
    format_claude_session_approval_decision_audit_json,
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
        help=f"Maximum flagged approvals to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--window-size",
        type=_positive_int,
        default=DEFAULT_WINDOW_SIZE,
        help=(
            "Number of subsequent events in the same session to inspect "
            f"(default: {DEFAULT_WINDOW_SIZE})."
        ),
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
                report = build_claude_session_approval_decision_audit_report(
                    conn,
                    limit=args.limit,
                    window_size=args.window_size,
                )
        else:
            with script_context() as (_config, db):
                report = build_claude_session_approval_decision_audit_report(
                    db,
                    limit=args.limit,
                    window_size=args.window_size,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_claude_session_approval_decision_audit_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
