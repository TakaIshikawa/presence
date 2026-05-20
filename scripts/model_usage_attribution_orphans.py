#!/usr/bin/env python3
"""Report model usage rows that cannot be attributed to content or pipeline runs."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.model_usage_attribution_orphans import (  # noqa: E402
    DEFAULT_LIMIT,
    build_model_usage_attribution_orphans_report_from_db,
    format_model_usage_attribution_orphans_json,
    format_model_usage_attribution_orphans_text,
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
    parser.add_argument("--details", action="store_true", help="Include representative orphan rows.")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {"include_details": args.details, "limit": args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_model_usage_attribution_orphans_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_model_usage_attribution_orphans_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_model_usage_attribution_orphans_text(report)
        if args.format == "text"
        else format_model_usage_attribution_orphans_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
