#!/usr/bin/env python3
"""Audit generated content for stale or missing source material."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.generated_source_freshness import (  # noqa: E402
    DEFAULT_AGING_DAYS,
    DEFAULT_STALE_DAYS,
    build_generated_source_freshness_report,
    format_generated_source_freshness_json,
    format_generated_source_freshness_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=int,
        help="Only include generated_content created within this many days.",
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=DEFAULT_STALE_DAYS,
        help=f"Classify content stale when newest source is this old (default: {DEFAULT_STALE_DAYS}).",
    )
    parser.add_argument(
        "--aging-days",
        type=int,
        default=DEFAULT_AGING_DAYS,
        help=f"Classify content aging when newest source is this old (default: {DEFAULT_AGING_DAYS}).",
    )
    parser.add_argument("--content-type", help="Only include this generated_content.content_type.")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_generated_source_freshness_report(
                    conn,
                    days=args.days,
                    stale_days=args.stale_days,
                    aging_days=args.aging_days,
                    content_type=args.content_type,
                )
        else:
            with script_context() as (_config, db):
                report = build_generated_source_freshness_report(
                    db,
                    days=args.days,
                    stale_days=args.stale_days,
                    aging_days=args.aging_days,
                    content_type=args.content_type,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_generated_source_freshness_json(report))
    else:
        print(format_generated_source_freshness_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
