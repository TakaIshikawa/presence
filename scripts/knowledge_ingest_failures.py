#!/usr/bin/env python3
"""Report curated sources with failed or missing knowledge ingestion."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_ingest_failures import (  # noqa: E402
    DEFAULT_DAYS,
    build_knowledge_source_ingest_failure_report,
    format_knowledge_source_ingest_failures_json,
    format_knowledge_source_ingest_failures_text,
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
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Recent knowledge window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--source-type",
        help="Limit report to one curated source type, such as x_account, blog, or newsletter.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--include-healthy",
        action="store_true",
        help="Include sources with recent successful knowledge ingestion.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_knowledge_source_ingest_failure_report(
                    conn,
                    days=args.days,
                    source_type=args.source_type,
                    include_healthy=args.include_healthy,
                )
        else:
            with script_context() as (_config, db):
                report = build_knowledge_source_ingest_failure_report(
                    db,
                    days=args.days,
                    source_type=args.source_type,
                    include_healthy=args.include_healthy,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_knowledge_source_ingest_failures_json(report))
    else:
        print(format_knowledge_source_ingest_failures_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
