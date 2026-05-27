#!/usr/bin/env python3
"""Report newsletter CTA destination mismatches."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_cta_destination_mismatch import build_newsletter_cta_destination_mismatch_report_from_db, format_newsletter_cta_destination_mismatch_json, format_newsletter_cta_destination_mismatch_text  # noqa: E402
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_newsletter_cta_destination_mismatch_report_from_db(conn)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_cta_destination_mismatch_report_from_db(db)
    except (SystemExit, OSError, sqlite3.Error, ValueError) as exc:
        if isinstance(exc, SystemExit):
            return int(exc.code or 0)
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_newsletter_cta_destination_mismatch_text(report) if args.format == "text" else format_newsletter_cta_destination_mismatch_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
