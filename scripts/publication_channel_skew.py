#!/usr/bin/env python3
"""Report publication channel skew against target shares."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_channel_skew import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_publication_channel_skew_report,
    format_publication_channel_skew_json,
    format_publication_channel_skew_text,
    parse_target_json,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--target", help="JSON object of channel target shares.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        targets = parse_target_json(args.target)
    except (SystemExit, ValueError) as exc:
        if isinstance(exc, ValueError):
            print(f"error: {exc}", file=sys.stderr)
            return 2
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publication_channel_skew_report(conn, days=args.days, limit=args.limit, target=targets)
        else:
            with script_context() as (_config, db):
                report = build_publication_channel_skew_report(db, days=args.days, limit=args.limit, target=targets)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_publication_channel_skew_json(report) if args.json else format_publication_channel_skew_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
