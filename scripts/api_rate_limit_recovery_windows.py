#!/usr/bin/env python3
"""Report API rate-limit recovery windows from stored snapshots."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.api_rate_limit_recovery_windows import (  # noqa: E402
    DEFAULT_STALE_MINUTES,
    DEFAULT_THRESHOLD,
    build_api_rate_limit_recovery_windows_report_from_db,
    format_api_rate_limit_recovery_windows_json,
    format_api_rate_limit_recovery_windows_text,
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


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--threshold", type=_non_negative_int, default=DEFAULT_THRESHOLD)
    parser.add_argument("--stale-minutes", type=_positive_int, default=DEFAULT_STALE_MINUTES)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    kwargs = {"stale_minutes": args.stale_minutes, "threshold": args.threshold}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_api_rate_limit_recovery_windows_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_api_rate_limit_recovery_windows_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_api_rate_limit_recovery_windows_text(report)
        if args.format == "text"
        else format_api_rate_limit_recovery_windows_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
