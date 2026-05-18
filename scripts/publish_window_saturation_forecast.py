#!/usr/bin/env python3
"""Forecast upcoming publish-window saturation."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_window_saturation_forecast import (  # noqa: E402
    DEFAULT_CAPACITY,
    DEFAULT_DAYS,
    build_publish_window_saturation_forecast_report_from_db,
    format_publish_window_saturation_forecast_json,
    format_publish_window_saturation_forecast_text,
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
    parser.add_argument("--capacity", type=_positive_int, default=DEFAULT_CAPACITY)
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
                report = build_publish_window_saturation_forecast_report_from_db(
                    conn,
                    days=args.days,
                    capacity=args.capacity,
                )
        else:
            with script_context() as (_config, db):
                report = build_publish_window_saturation_forecast_report_from_db(
                    db,
                    days=args.days,
                    capacity=args.capacity,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_publish_window_saturation_forecast_text(report)
        if args.format == "text"
        else format_publish_window_saturation_forecast_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
