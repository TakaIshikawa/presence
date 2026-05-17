#!/usr/bin/env python3
"""Report published content calendar spacing by channel."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_calendar_spacing import (  # noqa: E402
    DEFAULT_BURST_THRESHOLD,
    DEFAULT_LONG_GAP_HOURS,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_UNEVEN_RATIO,
    build_content_calendar_spacing_report,
    format_content_calendar_spacing_json,
    format_content_calendar_spacing_table,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--long-gap-hours", type=_positive_float, default=DEFAULT_LONG_GAP_HOURS)
    parser.add_argument("--burst-threshold", type=_positive_int, default=DEFAULT_BURST_THRESHOLD)
    parser.add_argument("--uneven-ratio", type=_positive_float, default=DEFAULT_UNEVEN_RATIO)
    parser.add_argument("--format", choices=("json", "table"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_content_calendar_spacing_report(
                db,
                lookback_days=args.lookback_days,
                long_gap_hours=args.long_gap_hours,
                burst_threshold=args.burst_threshold,
                uneven_ratio=args.uneven_ratio,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_table = args.table or args.format == "table"
    print(format_content_calendar_spacing_table(report) if as_table else format_content_calendar_spacing_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
