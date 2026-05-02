#!/usr/bin/env python3
"""Report stale ingestion poll cursors."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.poll_state_freshness import (  # noqa: E402
    DEFAULT_STALE_HOURS,
    DEFAULT_WARNING_HOURS,
    build_poll_state_freshness_report,
    format_poll_state_freshness_json,
    format_poll_state_freshness_text,
)
from runner import script_context  # noqa: E402


def _non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--warning-hours",
        type=_non_negative_float,
        default=DEFAULT_WARNING_HOURS,
        help=f"Classify pollers as warning at this age (default: {DEFAULT_WARNING_HOURS:g}).",
    )
    parser.add_argument(
        "--stale-hours",
        type=_non_negative_float,
        default=DEFAULT_STALE_HOURS,
        help=f"Classify pollers as stale at this age (default: {DEFAULT_STALE_HOURS:g}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Only include this poller source. Can be provided multiple times.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_poll_state_freshness_report(
                db,
                warning_hours=args.warning_hours,
                stale_hours=args.stale_hours,
                sources=args.source,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_poll_state_freshness_json(report))
    else:
        print(format_poll_state_freshness_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
