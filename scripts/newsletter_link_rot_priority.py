#!/usr/bin/env python3
"""Report newsletter links that should be repaired first."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_link_rot_priority import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STALE_DAYS,
    build_newsletter_link_rot_priority_report,
    format_newsletter_link_rot_priority_json,
    format_newsletter_link_rot_priority_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--stale-days", type=_positive_int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_newsletter_link_rot_priority_report(
                    conn,
                    stale_days=args.stale_days,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_newsletter_link_rot_priority_report(
                    db,
                    stale_days=args.stale_days,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_newsletter_link_rot_priority_json(report)
        if args.format == "json"
        else format_newsletter_link_rot_priority_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
