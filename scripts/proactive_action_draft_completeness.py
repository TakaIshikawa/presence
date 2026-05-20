#!/usr/bin/env python3
"""Report proactive action draft completeness gaps."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.proactive_action_draft_completeness import (  # noqa: E402
    DEFAULT_ACTION_TYPE,
    DEFAULT_DISCOVERY_SOURCE,
    DEFAULT_STATUS,
    build_proactive_action_draft_completeness_report_from_db,
    format_proactive_action_draft_completeness_json,
    format_proactive_action_draft_completeness_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--status", default=DEFAULT_STATUS, help="Status or comma-separated statuses. Defaults to all.")
    parser.add_argument("--action-type", default=DEFAULT_ACTION_TYPE, help="Action type or comma-separated types. Defaults to all.")
    parser.add_argument(
        "--discovery-source",
        default=DEFAULT_DISCOVERY_SOURCE,
        help="Discovery source or comma-separated sources. Defaults to all.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    kwargs = {
        "status": args.status,
        "action_type": args.action_type,
        "discovery_source": args.discovery_source,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_proactive_action_draft_completeness_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_proactive_action_draft_completeness_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_proactive_action_draft_completeness_text(report)
        if args.format == "text"
        else format_proactive_action_draft_completeness_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
