#!/usr/bin/env python3
"""Report curated sources at risk of paywall or access gating."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.curated_source_paywall_risk import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_CONFIDENCE,
    build_curated_source_paywall_risk_report,
    format_curated_source_paywall_risk_json,
    format_curated_source_paywall_risk_text,
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


def _confidence(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--source-type")
    parser.add_argument("--min-confidence", type=_confidence, default=DEFAULT_MIN_CONFIDENCE)
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
                report = build_curated_source_paywall_risk_report(
                    conn,
                    days=args.days,
                    source_type=args.source_type,
                    min_confidence=args.min_confidence,
                )
        else:
            with script_context() as (_config, db):
                report = build_curated_source_paywall_risk_report(
                    db,
                    days=args.days,
                    source_type=args.source_type,
                    min_confidence=args.min_confidence,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_curated_source_paywall_risk_json(report))
    else:
        print(format_curated_source_paywall_risk_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
