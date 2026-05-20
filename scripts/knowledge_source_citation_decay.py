#!/usr/bin/env python3
"""Find knowledge sources with declining citation frequency."""

from __future__ import annotations

import argparse
from datetime import datetime
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_citation_decay import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_DROP,
    DEFAULT_MIN_DROP_PERCENT,
    DEFAULT_WINDOW_DAYS,
    build_source_citation_decay_report_from_db,
    format_source_citation_decay_json,
    format_source_citation_decay_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be an ISO datetime") from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--window-days", type=_positive_int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--min-drop", type=_non_negative_int, default=DEFAULT_MIN_DROP)
    parser.add_argument("--min-drop-percent", type=_non_negative_float, default=DEFAULT_MIN_DROP_PERCENT)
    parser.add_argument("--source-type")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--now", type=_datetime)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {
        "window_days": args.window_days,
        "min_drop": args.min_drop,
        "min_drop_percent": args.min_drop_percent,
        "source_type": args.source_type,
        "limit": args.limit,
        "now": args.now,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_source_citation_decay_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_source_citation_decay_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_source_citation_decay_text(report) if args.format == "text" else format_source_citation_decay_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
