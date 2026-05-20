#!/usr/bin/env python3
"""Audit model usage attribution links to content and pipeline runs."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.model_usage_pipeline_link_gaps import (  # noqa: E402
    DEFAULT_COST_THRESHOLD,
    DEFAULT_LIMIT,
    DEFAULT_WINDOW_HOURS,
    build_model_usage_pipeline_link_gaps_report_from_db,
    format_model_usage_pipeline_link_gaps_json,
    format_model_usage_pipeline_link_gaps_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--window-hours", type=_positive_int, default=DEFAULT_WINDOW_HOURS)
    parser.add_argument("--cost-threshold", type=_non_negative_float, default=DEFAULT_COST_THRESHOLD)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {"window_hours": args.window_hours, "cost_threshold": args.cost_threshold, "limit": args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_model_usage_pipeline_link_gaps_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_model_usage_pipeline_link_gaps_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_model_usage_pipeline_link_gaps_text(report) if args.format == "text" else format_model_usage_pipeline_link_gaps_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
