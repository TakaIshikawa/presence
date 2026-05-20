#!/usr/bin/env python3
"""Summarize proactive action conversion by discovery source."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.proactive_action_conversion_by_source import (  # noqa: E402
    build_proactive_action_conversion_by_source_report_from_db,
    format_proactive_action_conversion_by_source_json,
    format_proactive_action_conversion_by_source_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_proactive_action_conversion_by_source_report_from_db(conn)
        else:
            with script_context() as (_config, db):
                report = build_proactive_action_conversion_by_source_report_from_db(db)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_proactive_action_conversion_by_source_text(report) if args.format == "text" else format_proactive_action_conversion_by_source_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
