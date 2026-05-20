#!/usr/bin/env python3
"""Audit eval_results against eval_batches."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.eval_result_batch_integrity import (  # noqa: E402
    DEFAULT_DAYS,
    build_eval_result_batch_integrity_report_from_db,
    format_eval_result_batch_integrity_json,
    format_eval_result_batch_integrity_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--content-type", help="Only include one content type.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"days": args.days, "content_type": args.content_type}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_eval_result_batch_integrity_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_eval_result_batch_integrity_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_eval_result_batch_integrity_text(report) if args.format == "text" else format_eval_result_batch_integrity_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
