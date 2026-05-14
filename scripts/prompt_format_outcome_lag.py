#!/usr/bin/env python3
"""Report prompt formats whose engagement outcomes are late."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.prompt_format_outcome_lag import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_SAMPLE,
    DEFAULT_OUTCOME_WINDOW_DAYS,
    build_prompt_format_outcome_lag_report_from_db,
    format_prompt_format_outcome_lag_json,
    format_prompt_format_outcome_lag_text,
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
    parser.add_argument("--outcome-window-days", type=_positive_int, default=DEFAULT_OUTCOME_WINDOW_DAYS)
    parser.add_argument("--min-sample", type=_positive_int, default=DEFAULT_MIN_SAMPLE)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Print the human-readable table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_prompt_format_outcome_lag_report_from_db(
                db,
                outcome_window_days=args.outcome_window_days,
                min_sample=args.min_sample,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    as_text = args.table or args.format == "text"
    print(format_prompt_format_outcome_lag_text(report) if as_text else format_prompt_format_outcome_lag_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
