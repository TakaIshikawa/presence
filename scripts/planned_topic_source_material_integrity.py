#!/usr/bin/env python3
"""Report planned topic source material and lifecycle integrity issues."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.planned_topic_source_material_integrity import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_planned_topic_source_material_integrity_report_from_db,
    format_planned_topic_source_material_integrity_json,
    format_planned_topic_source_material_integrity_text,
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
    parser.add_argument("--status", help="Restrict to one planned_topics.status value.")
    parser.add_argument("--campaign-id", type=int, help="Restrict to one planned_topics.campaign_id value.")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
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
                conn.row_factory = sqlite3.Row
                report = build_planned_topic_source_material_integrity_report_from_db(
                    conn,
                    status=args.status,
                    campaign_id=args.campaign_id,
                    days=args.days,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_planned_topic_source_material_integrity_report_from_db(
                    db,
                    status=args.status,
                    campaign_id=args.campaign_id,
                    days=args.days,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_planned_topic_source_material_integrity_text(report)
        if args.format == "text"
        else format_planned_topic_source_material_integrity_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
