#!/usr/bin/env python3
"""Report few-shot example influence drift."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.few_shot_influence_drift import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_UNDERPERFORMANCE_PCT,
    DEFAULT_MIN_USES,
    build_few_shot_influence_drift_report,
    format_few_shot_influence_drift_json,
    format_few_shot_influence_drift_text,
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
    parser.add_argument("--min-uses", type=_positive_int, default=DEFAULT_MIN_USES)
    parser.add_argument("--min-underperformance-pct", type=_nonnegative_float, default=DEFAULT_MIN_UNDERPERFORMANCE_PCT)
    parser.add_argument("--content-type")
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
                report = build_few_shot_influence_drift_report(conn, days=args.days, min_uses=args.min_uses, min_underperformance_pct=args.min_underperformance_pct, content_type=args.content_type)
        else:
            with script_context() as (_config, db):
                report = build_few_shot_influence_drift_report(db, days=args.days, min_uses=args.min_uses, min_underperformance_pct=args.min_underperformance_pct, content_type=args.content_type)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_few_shot_influence_drift_json(report) if args.format == "json" else format_few_shot_influence_drift_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
