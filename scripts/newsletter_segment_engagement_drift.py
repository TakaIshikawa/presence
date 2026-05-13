#!/usr/bin/env python3
"""Report newsletter segment engagement drift."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_segment_engagement_drift import (  # noqa: E402
    DEFAULT_BASELINE_DAYS,
    DEFAULT_DAYS,
    DEFAULT_MIN_DELTA_PCT,
    build_newsletter_segment_engagement_drift_report,
    format_newsletter_segment_engagement_drift_json,
    format_newsletter_segment_engagement_drift_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--baseline-days", type=_positive_int, default=DEFAULT_BASELINE_DAYS)
    parser.add_argument("--segment")
    parser.add_argument("--min-delta-pct", type=_nonnegative_float, default=DEFAULT_MIN_DELTA_PCT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
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
                report = build_newsletter_segment_engagement_drift_report(conn, days=args.days, baseline_days=args.baseline_days, segment=args.segment, min_delta_pct=args.min_delta_pct)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_segment_engagement_drift_report(db, days=args.days, baseline_days=args.baseline_days, segment=args.segment, min_delta_pct=args.min_delta_pct)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_newsletter_segment_engagement_drift_json(report) if args.format == "json" else format_newsletter_segment_engagement_drift_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
