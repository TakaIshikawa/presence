#!/usr/bin/env python3
"""Report curated sources with no recent ingested knowledge."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.curated_source_ingestion_silence import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STALE_DAYS,
    build_curated_source_ingestion_silence_report_from_db,
    format_curated_source_ingestion_silence_json,
    format_curated_source_ingestion_silence_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--stale-days", type=_positive_int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"stale_days": args.stale_days, "limit": args.limit}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_curated_source_ingestion_silence_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_curated_source_ingestion_silence_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_curated_source_ingestion_silence_text(report)
        if args.format == "text"
        else format_curated_source_ingestion_silence_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
