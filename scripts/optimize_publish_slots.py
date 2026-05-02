#!/usr/bin/env python3
"""Recommend non-mutating publish queue moves into better open slots."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_slot_optimizer import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_publish_slot_optimizer_report,
    format_publish_slot_optimizer_json,
    format_publish_slot_optimizer_text,
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


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Look-ahead horizon in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        choices=("all", "x", "bluesky"),
        default="all",
        help="Platform to optimize (default: all).",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum move proposals to print (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
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
            report = build_publish_slot_optimizer_report(
                db,
                days=args.days,
                platform=args.platform,
                limit=args.limit,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publish_slot_optimizer_json(report))
    else:
        print(format_publish_slot_optimizer_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
