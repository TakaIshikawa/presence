#!/usr/bin/env python3
"""Plan alternate-platform failover for stuck publication targets."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_failover import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_CONFIDENCE,
    build_publish_failover_report,
    format_publish_failover_json,
    format_publish_failover_text,
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


def _confidence(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        choices=("all", "x", "bluesky"),
        default="all",
        help="Source platform to include (default: all).",
    )
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-confidence",
        type=_confidence,
        default=DEFAULT_MIN_CONFIDENCE,
        help=f"Minimum confidence score to include (default: {DEFAULT_MIN_CONFIDENCE:g}).",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            report = build_publish_failover_report(
                db,
                platform=args.platform,
                days=args.days,
                min_confidence=args.min_confidence,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_publish_failover_json(report))
    else:
        print(format_publish_failover_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
