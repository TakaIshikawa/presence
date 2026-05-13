#!/usr/bin/env python3
"""Report model and prompt cost regressions."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.model_prompt_cost_regression import (  # noqa: E402
    DEFAULT_BASELINE_DAYS,
    DEFAULT_DAYS,
    DEFAULT_MIN_COST_INCREASE_PCT,
    build_model_prompt_cost_regression_report,
    format_model_prompt_cost_regression_json,
    format_model_prompt_cost_regression_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--baseline-days", type=_positive_int, default=DEFAULT_BASELINE_DAYS)
    parser.add_argument("--min-cost-increase-pct", type=_nonnegative_float, default=DEFAULT_MIN_COST_INCREASE_PCT)
    parser.add_argument("--model")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_model_prompt_cost_regression_report(conn, days=args.days, baseline_days=args.baseline_days, min_cost_increase_pct=args.min_cost_increase_pct, model=args.model)
        else:
            with script_context() as (_config, db):
                report = build_model_prompt_cost_regression_report(db, days=args.days, baseline_days=args.baseline_days, min_cost_increase_pct=args.min_cost_increase_pct, model=args.model)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_model_prompt_cost_regression_json(report) if args.format == "json" else format_model_prompt_cost_regression_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
