#!/usr/bin/env python3
"""Report publication channel SLA breaches."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_channel_sla_breach import (  # noqa: E402
    build_publication_channel_sla_breach_report_from_db,
    format_publication_channel_sla_breach_json,
    format_publication_channel_sla_breach_text,
)
from runner import script_context  # noqa: E402


def _json_object(value: str) -> dict:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"invalid JSON: {value}") from exc
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("value must be a JSON object")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--threshold-hours", type=_json_object, help='JSON object, e.g. {"queued": 24}')
    parser.add_argument(
        "--channel-threshold-hours",
        type=_json_object,
        help='JSON object, e.g. {"x": {"queued": 12}}',
    )
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {
            "threshold_hours": args.threshold_hours,
            "channel_threshold_hours": args.channel_threshold_hours,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publication_channel_sla_breach_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_publication_channel_sla_breach_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_publication_channel_sla_breach_text(report)
        if args.format == "text"
        else format_publication_channel_sla_breach_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
