#!/usr/bin/env python3
"""Report reply review audit gaps."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_review_audit_gaps import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_STALE_PENDING_HOURS,
    build_reply_review_audit_gaps_report,
    format_reply_review_audit_gaps_json,
    format_reply_review_audit_gaps_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--stale-pending-hours", type=int, default=DEFAULT_STALE_PENDING_HOURS)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_reply_review_audit_gaps_report(
                db,
                days=args.days,
                stale_pending_hours=args.stale_pending_hours,
                limit=args.limit,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_reply_review_audit_gaps_json(report) if args.format == "json" else format_reply_review_audit_gaps_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
