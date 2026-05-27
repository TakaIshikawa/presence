#!/usr/bin/env python3
"""Import Instagram post insight exports."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.instagram_post_insight_import import (  # noqa: E402
    format_instagram_post_insight_import_json,
    format_instagram_post_insight_import_text,
    import_instagram_post_insights,
)
from runner import script_context  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--input", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                summary = import_instagram_post_insights(conn, args.input, dry_run=args.dry_run)
        else:
            with script_context() as (_config, db):
                summary = import_instagram_post_insights(db.conn, args.input, dry_run=args.dry_run)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_instagram_post_insight_import_text(summary)
        if args.format == "text"
        else format_instagram_post_insight_import_json(summary)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
