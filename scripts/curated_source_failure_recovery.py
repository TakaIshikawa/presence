#!/usr/bin/env python3
"""Report curated source failure recovery status."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.curated_source_failure_recovery import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_DAYS,
    build_curated_source_failure_recovery_report_from_db,
    format_curated_source_failure_recovery_json,
    format_curated_source_failure_recovery_text,
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
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--source-type", default="all", help="Comma-separated source type filter.")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {"lookback_days": args.lookback_days, "source_type": args.source_type, "limit": args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_curated_source_failure_recovery_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_curated_source_failure_recovery_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_curated_source_failure_recovery_text(report) if args.format == "text" else format_curated_source_failure_recovery_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
