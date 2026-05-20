#!/usr/bin/env python3
"""Report lag between newsletter sends and engagement metric snapshots."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_issue_metric_lag import (  # noqa: E402
    DEFAULT_FIRST_FETCH_SLA_HOURS,
    DEFAULT_MAX_REFRESH_AGE_HOURS,
    build_newsletter_issue_metric_lag_report,
    format_newsletter_issue_metric_lag_json,
    format_newsletter_issue_metric_lag_text,
)
from runner import script_context  # noqa: E402


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument(
        "--first-fetch-sla-hours",
        type=_positive_float,
        default=DEFAULT_FIRST_FETCH_SLA_HOURS,
    )
    parser.add_argument(
        "--max-refresh-age-hours",
        type=_positive_float,
        default=DEFAULT_MAX_REFRESH_AGE_HOURS,
    )
    parser.add_argument("--include-ok", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    kwargs = {
        "first_fetch_sla_hours": args.first_fetch_sla_hours,
        "max_refresh_age_hours": args.max_refresh_age_hours,
        "include_ok": args.include_ok,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_newsletter_issue_metric_lag_report(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_issue_metric_lag_report(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_newsletter_issue_metric_lag_text(report)
        if args.format == "text"
        else format_newsletter_issue_metric_lag_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
