#!/usr/bin/env python3
"""Detect blog posts with overly similar excerpts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_excerpt_similarity import (  # noqa: E402
    DEFAULT_THRESHOLD,
    build_blog_excerpt_similarity_report_from_db,
    format_blog_excerpt_similarity_json,
    format_blog_excerpt_similarity_text,
)
from runner import script_context  # noqa: E402


def _threshold(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid threshold: {value}") from exc
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("threshold must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--threshold", type=_threshold, default=DEFAULT_THRESHOLD)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_blog_excerpt_similarity_report_from_db(db, threshold=args.threshold)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_blog_excerpt_similarity_text(report) if args.format == "text" else format_blog_excerpt_similarity_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
