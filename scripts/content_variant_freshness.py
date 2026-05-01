#!/usr/bin/env python3
"""Recommend stale content variants for targeted refresh."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.content_variant_freshness import (  # noqa: E402
    DEFAULT_DAYS,
    build_content_variant_freshness_report,
    format_content_variant_freshness_json,
    format_content_variant_freshness_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--platform", help="Only inspect variants for this platform.")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Only include changes from the last N days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--mark-stale-dry-run",
        action="store_true",
        help="Include a stable plan for marking variants stale without writing updates.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_content_variant_freshness_report(
                    conn,
                    platform=args.platform,
                    days=args.days,
                    mark_stale_dry_run=args.mark_stale_dry_run,
                )
        else:
            with script_context() as (_config, db):
                report = build_content_variant_freshness_report(
                    db,
                    platform=args.platform,
                    days=args.days,
                    mark_stale_dry_run=args.mark_stale_dry_run,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_variant_freshness_json(report))
    else:
        print(format_content_variant_freshness_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
