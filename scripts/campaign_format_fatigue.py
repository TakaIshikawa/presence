#!/usr/bin/env python3
"""Detect campaigns or topics overusing the same content format."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_format_fatigue import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_DOMINANT_SHARE,
    DEFAULT_MIN_COUNT,
    build_campaign_format_fatigue_report,
    format_campaign_format_fatigue_json,
    format_campaign_format_fatigue_text,
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


def _share(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid share: {value}") from exc
    if parsed <= 0 or parsed > 1:
        raise argparse.ArgumentTypeError("dominant-share must be > 0 and <= 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Generated/published content lookback in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-count",
        type=_positive_int,
        default=DEFAULT_MIN_COUNT,
        help=f"Minimum group size to evaluate (default: {DEFAULT_MIN_COUNT}).",
    )
    parser.add_argument(
        "--dominant-share",
        type=_share,
        default=DEFAULT_DOMINANT_SHARE,
        help=f"Dominant format share threshold (default: {DEFAULT_DOMINANT_SHARE}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_campaign_format_fatigue_report(
                db,
                days=args.days,
                min_count=args.min_count,
                dominant_share=args.dominant_share,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_campaign_format_fatigue_json(report))
    else:
        print(format_campaign_format_fatigue_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
