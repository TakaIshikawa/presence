#!/usr/bin/env python3
"""Report consistency between review decisions, evaluator scores, and gates."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.review_decision_consistency import (  # noqa: E402
    DEFAULT_HIGH_SCORE_THRESHOLD,
    DEFAULT_LOW_SCORE_THRESHOLD,
    build_review_decision_consistency_report,
    format_review_decision_consistency_json,
    format_review_decision_consistency_table,
)
from runner import script_context  # noqa: E402


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--low-score-threshold", type=_non_negative_float, default=DEFAULT_LOW_SCORE_THRESHOLD)
    parser.add_argument("--high-score-threshold", type=_non_negative_float, default=DEFAULT_HIGH_SCORE_THRESHOLD)
    parser.add_argument("--format", choices=("json", "table"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        report = None
        with script_context() as (_config, db):
            report = build_review_decision_consistency_report(
                db,
                low_score_threshold=args.low_score_threshold,
                high_score_threshold=args.high_score_threshold,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_table = args.table or args.format == "table"
    print(format_review_decision_consistency_table(report) if as_table else format_review_decision_consistency_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
