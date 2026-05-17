#!/usr/bin/env python3
"""Report blog draft creation-to-publication lag."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_draft_publish_lag import (  # noqa: E402
    DEFAULT_STALE_DAYS,
    build_blog_draft_publish_lag_report_from_db,
    format_blog_draft_publish_lag_json,
    format_blog_draft_publish_lag_table,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stale-days", type=_non_negative_int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--format", choices=("json", "table", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_blog_draft_publish_lag_report_from_db(db, stale_days=args.stale_days)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_blog_draft_publish_lag_json(report) if args.format == "json" else format_blog_draft_publish_lag_table(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
