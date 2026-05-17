#!/usr/bin/env python3
"""Report refinement churn and final gate outcomes."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.refinement_gate_outcomes import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_HIGH_CHURN_ATTEMPTS,
    DEFAULT_LIMIT,
    build_refinement_gate_outcomes_report_from_db,
    format_refinement_gate_outcomes_json,
    format_refinement_gate_outcomes_text,
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--high-churn-attempts", type=_positive_int, default=DEFAULT_HIGH_CHURN_ATTEMPTS)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_refinement_gate_outcomes_report_from_db(
                db,
                days=args.days,
                limit=args.limit,
                high_churn_attempts=args.high_churn_attempts,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_refinement_gate_outcomes_text(report)
        if args.table or args.format == "text"
        else format_refinement_gate_outcomes_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
