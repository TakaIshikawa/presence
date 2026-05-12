#!/usr/bin/env python3
"""Report publication cadence variance by channel."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_cadence_variance import (  # noqa: E402
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_TARGETS,
    build_publish_cadence_variance_report,
    format_publish_cadence_variance_json,
    format_publish_cadence_variance_text,
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


def _target(value: str) -> tuple[str, float]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("target must be CHANNEL=COUNT_PER_WEEK")
    channel, raw_count = value.split("=", 1)
    channel = channel.strip().lower()
    if not channel:
        raise argparse.ArgumentTypeError("target channel must be non-empty")
    try:
        count = float(raw_count)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid target count: {raw_count}") from exc
    if count < 0:
        raise argparse.ArgumentTypeError("target count must be non-negative")
    return channel, count


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days",
        type=_positive_int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Publication lookback window in days (default: {DEFAULT_LOOKBACK_DAYS}).",
    )
    parser.add_argument(
        "--channel",
        action="append",
        default=[],
        help="Limit report to a channel. May be repeated.",
    )
    parser.add_argument(
        "--target",
        action="append",
        type=_target,
        default=[],
        help="Configured cadence target as CHANNEL=COUNT_PER_WEEK. May be repeated.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic JSON. Equivalent to --format json.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)
        targets = dict(args.target) if args.target else DEFAULT_TARGETS
        with script_context() as (_config, db):
            report = build_publish_cadence_variance_report(
                db,
                lookback_days=args.lookback_days,
                targets=targets,
                channels=tuple(args.channel) if args.channel else None,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json or args.format == "json":
        print(format_publish_cadence_variance_json(report))
    else:
        print(format_publish_cadence_variance_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
