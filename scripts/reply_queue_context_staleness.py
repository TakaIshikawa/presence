#!/usr/bin/env python3
"""Report queued reply drafts with stale context."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_queue_context_staleness import (  # noqa: E402
    DEFAULT_DRAFT_REVIEW_HOURS,
    DEFAULT_STALE_CONTEXT_HOURS,
    DEFAULT_STALE_SOURCE_HOURS,
    build_reply_queue_context_staleness_report,
    format_reply_queue_context_staleness_json,
    format_reply_queue_context_staleness_table,
)
from runner import script_context  # noqa: E402


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stale-context-hours", type=_positive_float, default=DEFAULT_STALE_CONTEXT_HOURS)
    parser.add_argument("--stale-source-hours", type=_positive_float, default=DEFAULT_STALE_SOURCE_HOURS)
    parser.add_argument("--draft-review-hours", type=_positive_float, default=DEFAULT_DRAFT_REVIEW_HOURS)
    parser.add_argument("--format", choices=("json", "table"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_reply_queue_context_staleness_report(
                db,
                stale_context_hours=args.stale_context_hours,
                stale_source_hours=args.stale_source_hours,
                draft_review_hours=args.draft_review_hours,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_table = args.table or args.format == "table"
    print(format_reply_queue_context_staleness_table(report) if as_table else format_reply_queue_context_staleness_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
