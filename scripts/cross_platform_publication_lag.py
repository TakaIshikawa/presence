#!/usr/bin/env python3
"""Report cross-platform publication lag."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.cross_platform_publication_lag import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_THRESHOLD_HOURS,
    build_cross_platform_publication_lag_report,
    format_cross_platform_publication_lag_json,
    format_cross_platform_publication_lag_text,
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
    parser.add_argument("--threshold-hours", type=_nonnegative_float, default=DEFAULT_THRESHOLD_HOURS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_cross_platform_publication_lag_report(
                db,
                threshold_hours=args.threshold_hours,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_cross_platform_publication_lag_json(report)
        if args.format == "json"
        else format_cross_platform_publication_lag_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
