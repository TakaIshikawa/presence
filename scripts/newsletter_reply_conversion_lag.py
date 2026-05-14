#!/usr/bin/env python3
"""Report lag from newsletter sends to reply-driven conversations."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_reply_conversion_lag import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STALE_AFTER_DAYS,
    build_newsletter_reply_conversion_lag_report_from_db,
    format_newsletter_reply_conversion_lag_json,
    format_newsletter_reply_conversion_lag_text,
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
    parser.add_argument("--stale-after-days", type=_positive_int, default=DEFAULT_STALE_AFTER_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Print the human-readable table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_newsletter_reply_conversion_lag_report_from_db(
                db,
                stale_after_days=args.stale_after_days,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    as_text = args.table or args.format == "text"
    print(
        format_newsletter_reply_conversion_lag_text(report)
        if as_text
        else format_newsletter_reply_conversion_lag_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
