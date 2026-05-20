#!/usr/bin/env python3
"""Report content campaign throughput SLA gaps."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_campaign_throughput_sla import (  # noqa: E402
    DEFAULT_GENERATED_AGE_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_OVERDUE_DAYS,
    build_content_campaign_throughput_sla_report_from_db,
    format_content_campaign_throughput_sla_json,
    format_content_campaign_throughput_sla_text,
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


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid datetime: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--now", type=_datetime, help="Current time for deterministic SLA calculations.")
    parser.add_argument("--overdue-days", type=_non_negative_int, default=DEFAULT_OVERDUE_DAYS)
    parser.add_argument("--generated-age-days", type=_non_negative_int, default=DEFAULT_GENERATED_AGE_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    kwargs = {
        "generated_age_days": args.generated_age_days,
        "limit": args.limit,
        "now": args.now,
        "overdue_days": args.overdue_days,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_content_campaign_throughput_sla_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_content_campaign_throughput_sla_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_content_campaign_throughput_sla_text(report)
        if args.format == "text"
        else format_content_campaign_throughput_sla_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
