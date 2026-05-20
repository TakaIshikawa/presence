#!/usr/bin/env python3
"""Report model_usage input/output token ratio drift."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.model_usage_token_ratio_drift import (  # noqa: E402
    DEFAULT_BASELINE_DAYS,
    DEFAULT_CURRENT_DAYS,
    DEFAULT_DRIFT_THRESHOLD,
    DEFAULT_LIMIT,
    DEFAULT_MIN_BASELINE_ROWS,
    build_model_usage_token_ratio_drift_report_from_db,
    format_model_usage_token_ratio_drift_json,
    format_model_usage_token_ratio_drift_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--current-days", type=_positive_int, default=DEFAULT_CURRENT_DAYS)
    parser.add_argument("--baseline-days", type=_positive_int, default=DEFAULT_BASELINE_DAYS)
    parser.add_argument("--min-baseline-rows", type=_positive_int, default=DEFAULT_MIN_BASELINE_ROWS)
    parser.add_argument("--drift-threshold", type=float, default=DEFAULT_DRIFT_THRESHOLD)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        kwargs = {
            "current_days": args.current_days,
            "baseline_days": args.baseline_days,
            "min_baseline_rows": args.min_baseline_rows,
            "drift_threshold": args.drift_threshold,
            "limit": args.limit,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_model_usage_token_ratio_drift_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_model_usage_token_ratio_drift_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError, SystemExit) as exc:
        if isinstance(exc, SystemExit):
            return int(exc.code or 0)
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_model_usage_token_ratio_drift_text(report) if args.format == "text" else format_model_usage_token_ratio_drift_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
