#!/usr/bin/env python3
"""Report published post hashtag overuse."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.published_post_hashtag_overuse import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_HASHTAGS_PER_POST,
    DEFAULT_MIN_DIVERSITY_RATIO,
    DEFAULT_REPEATED_SET_THRESHOLD,
    DEFAULT_WINDOW_DAYS,
    build_published_post_hashtag_overuse_report_from_db,
    format_published_post_hashtag_overuse_json,
    format_published_post_hashtag_overuse_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--window-days", type=_positive_int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--max-hashtags-per-post", type=_positive_int, default=DEFAULT_MAX_HASHTAGS_PER_POST)
    parser.add_argument("--repeated-set-threshold", type=_positive_int, default=DEFAULT_REPEATED_SET_THRESHOLD)
    parser.add_argument("--min-diversity-ratio", type=float, default=DEFAULT_MIN_DIVERSITY_RATIO)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {
        "window_days": args.window_days,
        "max_hashtags_per_post": args.max_hashtags_per_post,
        "repeated_set_threshold": args.repeated_set_threshold,
        "min_diversity_ratio": args.min_diversity_ratio,
        "limit": args.limit,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_published_post_hashtag_overuse_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_published_post_hashtag_overuse_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_published_post_hashtag_overuse_text(report) if args.format == "text" else format_published_post_hashtag_overuse_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
