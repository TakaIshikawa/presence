#!/usr/bin/env python3
"""Report blog draft CTAs that are missing or misaligned with draft theme."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_cta_alignment import (  # noqa: E402
    build_blog_cta_alignment_report,
    format_blog_cta_alignment_json,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--include-aligned",
        action="store_true",
        help="Include aligned rows instead of emitting only missing or mismatched CTAs.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_blog_cta_alignment_report(
                    conn,
                    include_aligned=args.include_aligned,
                )
        else:
            with script_context() as (_config, db):
                report = build_blog_cta_alignment_report(
                    db,
                    include_aligned=args.include_aligned,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_blog_cta_alignment_json(report))
    return 1 if report.blocking_issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
