#!/usr/bin/env python3
"""Report expected profile metric platform coverage."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.profile_metric_platform_coverage import (  # noqa: E402
    DEFAULT_EXPECTED_PLATFORMS,
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_STALE_DAYS,
    build_profile_metric_platform_coverage_report_from_db,
    format_profile_metric_platform_coverage_json,
    format_profile_metric_platform_coverage_text,
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
    parser.add_argument("--expected-platform", action="append", dest="expected_platforms")
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--stale-days", type=_positive_int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {
        "expected_platforms": args.expected_platforms or DEFAULT_EXPECTED_PLATFORMS,
        "lookback_days": args.lookback_days,
        "stale_days": args.stale_days,
        "limit": args.limit,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_profile_metric_platform_coverage_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_profile_metric_platform_coverage_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_profile_metric_platform_coverage_text(report) if args.format == "text" else format_profile_metric_platform_coverage_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
