#!/usr/bin/env python3
"""Export publication attempt retry reason drift."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_attempt_retry_reason_drift import (  # noqa: E402
    DEFAULT_BASELINE_DAYS,
    DEFAULT_CURRENT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_DELTA,
    build_publication_attempt_retry_reason_drift_report_from_db,
    format_publication_attempt_retry_reason_drift_json,
    format_publication_attempt_retry_reason_drift_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--baseline-days", type=_positive_int, default=DEFAULT_BASELINE_DAYS)
    parser.add_argument("--current-days", type=_positive_int, default=DEFAULT_CURRENT_DAYS)
    parser.add_argument("--min-delta", type=_share, default=DEFAULT_MIN_DELTA)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        report_kwargs = {
            "baseline_days": args.baseline_days,
            "current_days": args.current_days,
            "min_delta": args.min_delta,
            "limit": args.limit,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publication_attempt_retry_reason_drift_report_from_db(conn, **report_kwargs)
        else:
            with script_context() as (_config, db):
                report = build_publication_attempt_retry_reason_drift_report_from_db(db, **report_kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "text":
        print(format_publication_attempt_retry_reason_drift_text(report))
    else:
        print(format_publication_attempt_retry_reason_drift_json(report))
    return 0


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _share(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
