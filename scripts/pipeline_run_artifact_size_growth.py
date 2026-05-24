#!/usr/bin/env python3
"""Report pipeline run artifact size growth."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.pipeline_run_artifact_size_growth import (  # noqa: E402
    DEFAULT_GROWTH_RATIO,
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MAX_BYTES,
    build_pipeline_run_artifact_size_growth_report_from_db,
    format_pipeline_run_artifact_size_growth_json,
    format_pipeline_run_artifact_size_growth_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _gt_one(value: str) -> float:
    parsed = float(value)
    if parsed <= 1:
        raise argparse.ArgumentTypeError("value must be greater than 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--max-bytes", type=_positive_int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--growth-ratio", type=_gt_one, default=DEFAULT_GROWTH_RATIO)
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"max_bytes": args.max_bytes, "growth_ratio": args.growth_ratio, "lookback_days": args.lookback_days, "limit": args.limit}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_pipeline_run_artifact_size_growth_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_pipeline_run_artifact_size_growth_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_pipeline_run_artifact_size_growth_text(report) if args.format == "text" else format_pipeline_run_artifact_size_growth_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
