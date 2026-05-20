#!/usr/bin/env python3
"""Report synthesis pipeline runs with low candidate diversity."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.pipeline_candidate_diversity_gaps import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_UNIQUE_FORMATS,
    DEFAULT_SIMILARITY_THRESHOLD,
    build_pipeline_candidate_diversity_gaps_report_from_db,
    format_pipeline_candidate_diversity_gaps_json,
    format_pipeline_candidate_diversity_gaps_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _threshold(value: str) -> float:
    parsed = float(value)
    if not 0 < parsed <= 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--min-unique-formats", type=_positive_int, default=DEFAULT_MIN_UNIQUE_FORMATS)
    parser.add_argument("--similarity-threshold", type=_threshold, default=DEFAULT_SIMILARITY_THRESHOLD)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"days": args.days, "min_unique_formats": args.min_unique_formats, "similarity_threshold": args.similarity_threshold, "limit": args.limit}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_pipeline_candidate_diversity_gaps_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_pipeline_candidate_diversity_gaps_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_pipeline_candidate_diversity_gaps_text(report) if args.format == "text" else format_pipeline_candidate_diversity_gaps_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
