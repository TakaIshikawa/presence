#!/usr/bin/env python3
"""Report Claude Code session file-editing tool outcomes."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.claude_session_file_edit_outcomes import (  # noqa: E402
    build_claude_session_file_edit_outcomes_report,
    format_claude_session_file_edit_outcomes_json,
    format_claude_session_file_edit_outcomes_text,
    load_claude_session_log_rows,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--log", help="Claude session JSONL log path.")
    source.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--output",
        help="Write deterministic JSON report to this path while printing text to stdout.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        if args.log:
            report = build_claude_session_file_edit_outcomes_report(
                load_claude_session_log_rows(args.log)
            )
        elif args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_claude_session_file_edit_outcomes_report(conn)
        else:
            with script_context() as (_config, db):
                report = build_claude_session_file_edit_outcomes_report(db)

        if args.output:
            Path(args.output).write_text(
                format_claude_session_file_edit_outcomes_json(report) + "\n",
                encoding="utf-8",
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_claude_session_file_edit_outcomes_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
