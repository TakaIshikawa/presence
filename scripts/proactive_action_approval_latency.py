#!/usr/bin/env python3
"""Report proactive action approval latency."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.proactive_action_approval_latency import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_PENDING_SLA_HOURS,
    DEFAULT_REVIEW_SLA_HOURS,
    DEFAULT_WINDOW_DAYS,
    build_proactive_action_approval_latency_report_from_db,
    format_proactive_action_approval_latency_json,
    format_proactive_action_approval_latency_text,
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
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--window-days", type=_positive_int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--pending-sla-hours", type=_positive_float, default=DEFAULT_PENDING_SLA_HOURS)
    parser.add_argument("--review-sla-hours", type=_positive_float, default=DEFAULT_REVIEW_SLA_HOURS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        kwargs = {
            "window_days": args.window_days,
            "pending_sla_hours": args.pending_sla_hours,
            "review_sla_hours": args.review_sla_hours,
            "limit": args.limit,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_proactive_action_approval_latency_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_proactive_action_approval_latency_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_proactive_action_approval_latency_text(report)
        if args.format == "text"
        else format_proactive_action_approval_latency_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
