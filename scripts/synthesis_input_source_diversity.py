#!/usr/bin/env python3
"""Report synthesis runs dominated by one input source type."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.synthesis_input_source_diversity import (  # noqa: E402
    DEFAULT_DOMINANCE_THRESHOLD,
    DEFAULT_LIMIT,
    build_synthesis_input_source_diversity_report_from_db,
    format_synthesis_input_source_diversity_json,
    format_synthesis_input_source_diversity_text,
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


def _threshold(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dominance-threshold", type=_threshold, default=DEFAULT_DOMINANCE_THRESHOLD)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_synthesis_input_source_diversity_report_from_db(
                db,
                dominance_threshold=args.dominance_threshold,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_synthesis_input_source_diversity_text(report)
        if args.table or args.format == "text"
        else format_synthesis_input_source_diversity_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
