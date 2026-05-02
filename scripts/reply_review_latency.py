#!/usr/bin/env python3
"""Report reply review latency and SLA breaches."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.reply_review_latency import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_SLA_HOURS,
    GROUP_FIELDS,
    build_reply_review_latency_report,
    format_reply_review_latency_json,
    format_reply_review_latency_text,
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
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--sla-hours",
        type=_positive_float,
        default=DEFAULT_SLA_HOURS,
        help=f"First-review SLA threshold in hours (default: {DEFAULT_SLA_HOURS:g}).",
    )
    parser.add_argument(
        "--group-by",
        choices=GROUP_FIELDS,
        default="platform",
        help="Dimension to group by (default: platform).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING if args.format == "json" else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            report = build_reply_review_latency_report(
                db,
                days=args.days,
                sla_hours=args.sla_hours,
                group_by=args.group_by,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_review_latency_json(report))
    else:
        print(format_reply_review_latency_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
