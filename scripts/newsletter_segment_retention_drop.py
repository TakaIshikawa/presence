#!/usr/bin/env python3
"""Export Newsletter Segment Retention Drop."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_segment_retention_drop import (  # noqa: E402
    DEFAULT_BASELINE_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_LOSS_RATE,
    DEFAULT_WINDOW_DAYS,
    build_newsletter_segment_retention_drop_report_from_db,
    format_newsletter_segment_retention_drop_json,
    format_newsletter_segment_retention_drop_text,
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


def _non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--window-days", type=_positive_int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--baseline-days", type=_positive_int, default=DEFAULT_BASELINE_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--min-loss-rate", type=_non_negative_float, default=DEFAULT_MIN_LOSS_RATE)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {
            "window_days": args.window_days,
            "baseline_days": args.baseline_days,
            "limit": args.limit,
            "min_loss_rate": args.min_loss_rate,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_newsletter_segment_retention_drop_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_segment_retention_drop_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_newsletter_segment_retention_drop_text(report)
        if args.format == "text"
        else format_newsletter_segment_retention_drop_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
