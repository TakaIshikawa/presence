#!/usr/bin/env python3
"""Report newsletter link anchor text quality issues."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_link_anchor_text_quality import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_ANCHOR_LENGTH,
    build_newsletter_link_anchor_text_quality_report_from_db,
    format_newsletter_link_anchor_text_quality_json,
    format_newsletter_link_anchor_text_quality_text,
)
from runner import script_context  # noqa: E402


def _positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--max-anchor-length", type=_positive, default=DEFAULT_MAX_ANCHOR_LENGTH)
    parser.add_argument("--limit", type=_positive, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"max_anchor_length": args.max_anchor_length, "limit": args.limit}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_newsletter_link_anchor_text_quality_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_link_anchor_text_quality_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_newsletter_link_anchor_text_quality_text(report)
        if args.format == "text"
        else format_newsletter_link_anchor_text_quality_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
