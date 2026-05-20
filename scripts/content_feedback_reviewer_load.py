#!/usr/bin/env python3
"""Report unresolved content feedback load by reviewer."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_feedback_reviewer_load import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_UNRESOLVED_STATUSES,
    build_content_feedback_reviewer_load_report_from_db,
    format_content_feedback_reviewer_load_json,
    format_content_feedback_reviewer_load_text,
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
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument(
        "--unresolved-statuses",
        default=",".join(DEFAULT_UNRESOLVED_STATUSES),
        help="Comma-separated statuses to include. Defaults to all statuses except resolved/closed values.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    kwargs = {"limit": args.limit, "unresolved_statuses": args.unresolved_statuses}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_content_feedback_reviewer_load_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_content_feedback_reviewer_load_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_content_feedback_reviewer_load_text(report)
        if args.format == "text"
        else format_content_feedback_reviewer_load_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
