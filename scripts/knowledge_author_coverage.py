#!/usr/bin/env python3
"""Report overrepresented and missing source authors in curated knowledge."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.author_coverage_report import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_DOMINANCE_THRESHOLD,
    DEFAULT_MIN_ENTRIES,
    DEFAULT_RECENT_DAYS,
    build_knowledge_author_coverage_report,
    format_knowledge_author_coverage_csv,
    format_knowledge_author_coverage_json,
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


def _threshold(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Look back at curated knowledge rows (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-entries",
        type=_positive_int,
        default=DEFAULT_MIN_ENTRIES,
        help=f"Minimum entries before an author is healthy (default: {DEFAULT_MIN_ENTRIES}).",
    )
    parser.add_argument(
        "--dominance-threshold",
        type=_threshold,
        default=DEFAULT_DOMINANCE_THRESHOLD,
        help=(
            "Share at or above which an author is dominant, from 0 to 1 "
            f"(default: {DEFAULT_DOMINANCE_THRESHOLD})."
        ),
    )
    parser.add_argument(
        "--recent-days",
        type=_positive_int,
        default=DEFAULT_RECENT_DAYS,
        help=f"Recency window for active author coverage (default: {DEFAULT_RECENT_DAYS}).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (default: json).",
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
                report = build_knowledge_author_coverage_report(
                    conn,
                    days=args.days,
                    min_entries=args.min_entries,
                    dominance_threshold=args.dominance_threshold,
                    recent_days=args.recent_days,
                )
        else:
            with script_context() as (_config, db):
                report = build_knowledge_author_coverage_report(
                    db,
                    days=args.days,
                    min_entries=args.min_entries,
                    dominance_threshold=args.dominance_threshold,
                    recent_days=args.recent_days,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        print(format_knowledge_author_coverage_csv(report))
    else:
        print(format_knowledge_author_coverage_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
