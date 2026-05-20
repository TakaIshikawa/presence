#!/usr/bin/env python3
"""Report recurring publication attempt errors."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_attempt_error_recurrence import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_COUNT,
    build_publication_attempt_error_recurrence_report_from_db,
    format_publication_attempt_error_recurrence_json,
    format_publication_attempt_error_recurrence_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--min-count", type=int, default=DEFAULT_MIN_COUNT)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {"min_count": args.min_count, "limit": args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_publication_attempt_error_recurrence_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_publication_attempt_error_recurrence_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_publication_attempt_error_recurrence_text(report)
        if args.format == "text"
        else format_publication_attempt_error_recurrence_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
