#!/usr/bin/env python3
"""Audit proactive action target URLs for shape and platform consistency."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.proactive_action_target_url_health import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STATUS,
    build_proactive_action_target_url_health_report_from_db,
    format_proactive_action_target_url_health_json,
    format_proactive_action_target_url_health_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--status", default=DEFAULT_STATUS)
    parser.add_argument("--platform")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"status": args.status or None, "platform": args.platform, "limit": args.limit}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_proactive_action_target_url_health_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_proactive_action_target_url_health_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_proactive_action_target_url_health_text(report) if args.format == "text" else format_proactive_action_target_url_health_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
