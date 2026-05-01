#!/usr/bin/env python3
"""Rebalance queued publish items away from daily cap violations."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.queue_rebalancer import (  # noqa: E402
    DEFAULT_REBALANCE_DAYS,
    apply_publish_queue_rebalance,
    format_queue_rebalance_report_json,
    format_queue_rebalance_report_text,
    parse_quiet_hours,
    plan_publish_queue_rebalance,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_REBALANCE_DAYS,
        help=f"Number of days in the rebalance window (default: {DEFAULT_REBALANCE_DAYS})",
    )
    parser.add_argument(
        "--platform",
        choices=["all", "x", "bluesky"],
        default="all",
        help="Platform to rebalance (default: all)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Update eligible queued rows instead of reporting a dry run",
    )
    parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--quiet-hours",
        help="Comma-separated UTC quiet-hour ranges to avoid, e.g. 22:00-06:00",
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
        quiet_hours = parse_quiet_hours(args.quiet_hours)
        with script_context() as (config, db):
            report = plan_publish_queue_rebalance(
                db,
                config,
                days=args.days,
                platform=args.platform,
                quiet_hours=quiet_hours,
            )
            if args.apply:
                report = apply_publish_queue_rebalance(db, report)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_queue_rebalance_report_json(report))
    else:
        print(format_queue_rebalance_report_text(report, applied=args.apply))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
