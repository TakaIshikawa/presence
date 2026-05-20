#!/usr/bin/env python3
"""Report prompt versions used without recent evaluation coverage."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.prompt_version_evaluation_coverage import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_DAYS,
    build_prompt_version_evaluation_coverage_report_from_db,
    format_prompt_version_evaluation_coverage_json,
    format_prompt_version_evaluation_coverage_text,
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
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
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
                report = build_prompt_version_evaluation_coverage_report_from_db(conn, lookback_days=args.lookback_days, limit=args.limit)
        else:
            with script_context() as (_config, db):
                report = build_prompt_version_evaluation_coverage_report_from_db(db, lookback_days=args.lookback_days, limit=args.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_prompt_version_evaluation_coverage_text(report) if args.format == "text" else format_prompt_version_evaluation_coverage_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
