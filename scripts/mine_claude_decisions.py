#!/usr/bin/env python3
"""Mine recent Claude Code messages for explicit technical decisions."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.claude_decision_miner import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_CONFIDENCE,
    build_claude_decision_miner_report,
    format_claude_decision_miner_json,
    format_claude_decision_miner_text,
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


def _positive_confidence(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid confidence: {value}") from exc
    if parsed <= 0 or parsed > 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for Claude messages (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-confidence",
        type=_positive_confidence,
        default=DEFAULT_MIN_CONFIDENCE,
        help=(
            "Minimum confidence required for a mined decision "
            f"(default: {DEFAULT_MIN_CONFIDENCE})."
        ),
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
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)
        with script_context() as (_config, db):
            report = build_claude_decision_miner_report(
                db,
                days=args.days,
                min_confidence=args.min_confidence,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_claude_decision_miner_json(report))
    else:
        print(format_claude_decision_miner_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
