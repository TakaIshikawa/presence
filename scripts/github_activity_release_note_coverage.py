#!/usr/bin/env python3
"""Report GitHub activity release note coverage gaps."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.github_activity_release_note_coverage import build_github_activity_release_note_coverage_report_from_db, format_github_activity_release_note_coverage_json, format_github_activity_release_note_coverage_text  # noqa: E402
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--significance-threshold", type=int, default=5)
    parser.add_argument("--late-days", type=int, default=7)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        kwargs = {"significance_threshold": args.significance_threshold, "late_days": args.late_days}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_github_activity_release_note_coverage_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_github_activity_release_note_coverage_report_from_db(db, **kwargs)
    except (SystemExit, OSError, sqlite3.Error, ValueError) as exc:
        if isinstance(exc, SystemExit):
            return int(exc.code or 0)
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_github_activity_release_note_coverage_text(report) if args.format == "text" else format_github_activity_release_note_coverage_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
