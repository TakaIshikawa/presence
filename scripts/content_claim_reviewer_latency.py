#!/usr/bin/env python3
"""Report content claim reviewer latency."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_claim_reviewer_latency import (  # noqa: E402
    DEFAULT_DECISION_THRESHOLD_HOURS,
    DEFAULT_LIMIT,
    DEFAULT_PENDING_THRESHOLD_HOURS,
    build_content_claim_reviewer_latency_report_from_db,
    format_content_claim_reviewer_latency_json,
    format_content_claim_reviewer_latency_text,
)
from runner import script_context  # noqa: E402


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--pending-threshold-hours", type=_positive_float, default=DEFAULT_PENDING_THRESHOLD_HOURS)
    parser.add_argument("--decision-threshold-hours", type=_positive_float, default=DEFAULT_DECISION_THRESHOLD_HOURS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"pending_threshold_hours": args.pending_threshold_hours, "decision_threshold_hours": args.decision_threshold_hours, "limit": args.limit}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_content_claim_reviewer_latency_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_content_claim_reviewer_latency_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_content_claim_reviewer_latency_text(report) if args.format == "text" else format_content_claim_reviewer_latency_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
