#!/usr/bin/env python3
"""Report generated content backed by stale source evidence."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.source_evidence_aging_risk import (  # noqa: E402
    DEFAULT_EXPIRED_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_STALE_DAYS,
    build_source_evidence_aging_risk_report_from_db,
    format_source_evidence_aging_risk_json,
    format_source_evidence_aging_risk_text,
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
    parser.add_argument("--stale-days", type=_positive_int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--expired-days", type=_positive_int, default=DEFAULT_EXPIRED_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    parser.add_argument("--table", action="store_true", help="Print the human-readable table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_source_evidence_aging_risk_report_from_db(
                db,
                stale_days=args.stale_days,
                expired_days=args.expired_days,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_text = args.table or args.format == "text"
    print(format_source_evidence_aging_risk_text(report) if as_text else format_source_evidence_aging_risk_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
