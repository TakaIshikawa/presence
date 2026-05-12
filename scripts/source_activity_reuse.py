#!/usr/bin/env python3
"""Export source activity reuse report."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.source_activity_reuse import (  # noqa: E402
    DEFAULT_CRITICAL_THRESHOLD,
    DEFAULT_DAYS,
    DEFAULT_WARNING_THRESHOLD,
    build_source_activity_reuse_report,
    format_source_activity_reuse_json,
    format_source_activity_reuse_text,
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
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--warning-threshold", type=_positive_int, default=DEFAULT_WARNING_THRESHOLD)
    parser.add_argument("--critical-threshold", type=_positive_int, default=DEFAULT_CRITICAL_THRESHOLD)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {
            "days": args.days,
            "warning_threshold": args.warning_threshold,
            "critical_threshold": args.critical_threshold,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_source_activity_reuse_report(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_source_activity_reuse_report(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_source_activity_reuse_json(report)
        if args.format == "json"
        else format_source_activity_reuse_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
