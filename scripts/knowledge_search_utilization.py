#!/usr/bin/env python3
"""Report utilization of retrieved knowledge search results."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.knowledge_search_utilization import (  # noqa: E402
    DEFAULT_LOW_UTILIZATION_RATE,
    DEFAULT_TOP_RANK,
    build_knowledge_search_utilization_report_from_db,
    format_knowledge_search_utilization_json,
    format_knowledge_search_utilization_table,
)
from runner import script_context  # noqa: E402


def _rate(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid rate: {value}") from exc
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("rate must be between 0 and 1")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--low-utilization-rate", type=_rate, default=DEFAULT_LOW_UTILIZATION_RATE)
    parser.add_argument("--top-rank", type=_positive_int, default=DEFAULT_TOP_RANK)
    parser.add_argument("--format", choices=("json", "table"), default="json")
    parser.add_argument("--table", action="store_true", help="Print table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_knowledge_search_utilization_report_from_db(
                db,
                low_utilization_rate=args.low_utilization_rate,
                top_rank=args.top_rank,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_knowledge_search_utilization_table(report)
        if args.table or args.format == "table"
        else format_knowledge_search_utilization_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
