#!/usr/bin/env python3
"""Report profile metrics platform snapshot gaps."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.profile_metrics_platform_snapshot_gaps import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_AGE_HOURS,
    DEFAULT_MAX_GAP_HOURS,
    DEFAULT_PLATFORM,
    build_profile_metrics_platform_snapshot_gaps_report_from_db,
    format_profile_metrics_platform_snapshot_gaps_json,
    format_profile_metrics_platform_snapshot_gaps_text,
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


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--platform", default=DEFAULT_PLATFORM)
    parser.add_argument("--max-age-hours", type=_positive_float, default=DEFAULT_MAX_AGE_HOURS)
    parser.add_argument("--max-gap-hours", type=_positive_float, default=DEFAULT_MAX_GAP_HOURS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    kwargs = {
        "platform": args.platform,
        "max_age_hours": args.max_age_hours,
        "max_gap_hours": args.max_gap_hours,
        "limit": args.limit,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_profile_metrics_platform_snapshot_gaps_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_profile_metrics_platform_snapshot_gaps_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_profile_metrics_platform_snapshot_gaps_text(report)
        if args.format == "text"
        else format_profile_metrics_platform_snapshot_gaps_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
