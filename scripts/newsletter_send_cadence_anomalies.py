#!/usr/bin/env python3
"""Report newsletter send cadence anomalies."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_send_cadence_anomalies import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_TARGET_DAYS,
    DEFAULT_TOLERANCE_HOURS,
    build_newsletter_send_cadence_anomalies_report,
    format_newsletter_send_cadence_anomalies_json,
    format_newsletter_send_cadence_anomalies_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--target-days", type=_positive_float, default=DEFAULT_TARGET_DAYS)
    parser.add_argument("--tolerance-hours", type=_positive_float, default=DEFAULT_TOLERANCE_HOURS)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Print text output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_newsletter_send_cadence_anomalies_report(
                db,
                days=args.days,
                target_days=args.target_days,
                tolerance_hours=args.tolerance_hours,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_text = args.table or args.format == "text"
    print(format_newsletter_send_cadence_anomalies_text(report) if as_text else format_newsletter_send_cadence_anomalies_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
