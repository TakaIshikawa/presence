#!/usr/bin/env python3
"""Report selected newsletter subjects whose engagement outcomes are late."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_subject_outcome_lag import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_AGE_HOURS,
    DEFAULT_STALE_AFTER_HOURS,
    build_newsletter_subject_outcome_lag_report,
    format_newsletter_subject_outcome_lag_json,
    format_newsletter_subject_outcome_lag_text,
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
    parser.add_argument("--min-age-hours", type=_positive_int, default=DEFAULT_MIN_AGE_HOURS)
    parser.add_argument("--stale-after-hours", type=_positive_int, default=DEFAULT_STALE_AFTER_HOURS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_newsletter_subject_outcome_lag_report(
                db,
                min_age_hours=args.min_age_hours,
                stale_after_hours=args.stale_after_hours,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_newsletter_subject_outcome_lag_json(report)
        if args.format == "json"
        else format_newsletter_subject_outcome_lag_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
