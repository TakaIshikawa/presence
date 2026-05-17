#!/usr/bin/env python3
"""Summarize publish outcomes by channel."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_channel_outcome_mix import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_FAILURE_THRESHOLD,
    DEFAULT_PENDING_THRESHOLD,
    build_publication_channel_outcome_mix_report_from_db,
    format_publication_channel_outcome_mix_json,
    format_publication_channel_outcome_mix_table,
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


def _ratio(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ratio: {value}") from exc
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("ratio must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--failure-threshold", type=_ratio, default=DEFAULT_FAILURE_THRESHOLD)
    parser.add_argument("--pending-threshold", type=_ratio, default=DEFAULT_PENDING_THRESHOLD)
    parser.add_argument("--format", choices=("json", "table", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_publication_channel_outcome_mix_report_from_db(
                db,
                days=args.days,
                failure_threshold=args.failure_threshold,
                pending_threshold=args.pending_threshold,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_publication_channel_outcome_mix_json(report) if args.format == "json" else format_publication_channel_outcome_mix_table(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
