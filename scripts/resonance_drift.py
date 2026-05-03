#!/usr/bin/env python3
"""Emit engagement resonance drift JSON for published posts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.resonance_drift import (  # noqa: E402
    DEFAULT_BASELINE_DAYS,
    DEFAULT_BUCKET_DAYS,
    DEFAULT_RECENT_DAYS,
    build_resonance_drift_report,
    format_resonance_drift_json,
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
    parser.add_argument(
        "--recent-days",
        type=_positive_int,
        default=DEFAULT_RECENT_DAYS,
        help=f"Recent comparison window in days (default: {DEFAULT_RECENT_DAYS}).",
    )
    parser.add_argument(
        "--baseline-days",
        type=_positive_int,
        default=DEFAULT_BASELINE_DAYS,
        help=f"Baseline window before the recent window in days (default: {DEFAULT_BASELINE_DAYS}).",
    )
    parser.add_argument(
        "--bucket-days",
        type=_positive_int,
        default=DEFAULT_BUCKET_DAYS,
        help=f"Aggregation bucket size in days (default: {DEFAULT_BUCKET_DAYS}).",
    )
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
                report = build_resonance_drift_report(
                    conn,
                    recent_days=args.recent_days,
                    baseline_days=args.baseline_days,
                    bucket_days=args.bucket_days,
                )
        else:
            with script_context() as (_config, db):
                report = build_resonance_drift_report(
                    db,
                    recent_days=args.recent_days,
                    baseline_days=args.baseline_days,
                    bucket_days=args.bucket_days,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_resonance_drift_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
