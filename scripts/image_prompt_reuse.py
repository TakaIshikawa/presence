#!/usr/bin/env python3
"""Audit reused image generation prompts."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.image_prompt_reuse import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_REUSE,
    DEFAULT_SIMILARITY_THRESHOLD,
    build_image_prompt_reuse_report,
    format_image_prompt_reuse_csv,
    format_image_prompt_reuse_json,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back by generated_content.created_at (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--min-reuse",
        type=int,
        default=DEFAULT_MIN_REUSE,
        help=f"Minimum rows required for a reuse bucket (default: {DEFAULT_MIN_REUSE})",
    )
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=DEFAULT_SIMILARITY_THRESHOLD,
        help=f"SequenceMatcher threshold for near duplicates (default: {DEFAULT_SIMILARITY_THRESHOLD})",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="csv",
        help="Output format (default: csv)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_image_prompt_reuse_report(
                    conn,
                    days=args.days,
                    min_reuse=args.min_reuse,
                    similarity_threshold=args.similarity_threshold,
                )
        else:
            with script_context() as (_config, db):
                report = build_image_prompt_reuse_report(
                    db,
                    days=args.days,
                    min_reuse=args.min_reuse,
                    similarity_threshold=args.similarity_threshold,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_image_prompt_reuse_json(report))
    else:
        print(format_image_prompt_reuse_csv(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
