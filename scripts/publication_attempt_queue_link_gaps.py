#!/usr/bin/env python3
"""Report publication attempt queue link gaps."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_attempt_queue_link_gaps import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_PLATFORM,
    build_publication_attempt_queue_link_gaps_report_from_db,
    format_publication_attempt_queue_link_gaps_json,
    format_publication_attempt_queue_link_gaps_text,
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
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--platform", default=DEFAULT_PLATFORM)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        kwargs = {"days": args.days, "platform": args.platform, "limit": args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publication_attempt_queue_link_gaps_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_publication_attempt_queue_link_gaps_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_publication_attempt_queue_link_gaps_text(report)
        if args.format == "text"
        else format_publication_attempt_queue_link_gaps_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
