#!/usr/bin/env python3
"""Export post format performance mix report."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.post_format_performance_mix import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_DELTA,
    DEFAULT_MIN_SAMPLES,
    build_post_format_performance_mix_report,
    format_post_format_performance_mix_json,
    format_post_format_performance_mix_text,
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


def _nonnegative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--min-samples", type=_positive_int, default=DEFAULT_MIN_SAMPLES)
    parser.add_argument("--delta", type=_nonnegative_float, default=DEFAULT_DELTA)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {"days": args.days, "min_samples": args.min_samples, "delta": args.delta}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_post_format_performance_mix_report(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_post_format_performance_mix_report(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_post_format_performance_mix_json(report)
        if args.format == "json"
        else format_post_format_performance_mix_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
