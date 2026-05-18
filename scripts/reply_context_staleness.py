#!/usr/bin/env python3
"""Report reply drafts whose context is stale at draft creation."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_context_staleness import (  # noqa: E402
    DEFAULT_OLD_HOURS,
    DEFAULT_STALE_HOURS,
    build_reply_context_staleness_report_from_db,
    format_reply_context_staleness_json,
    format_reply_context_staleness_table,
)
from runner import script_context  # noqa: E402


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-hours", type=_positive_float, default=DEFAULT_OLD_HOURS)
    parser.add_argument("--stale-hours", type=_positive_float, default=DEFAULT_STALE_HOURS)
    parser.add_argument("--format", choices=("json", "table"), default="json")
    parser.add_argument("--table", action="store_true", help="Print table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_reply_context_staleness_report_from_db(
                db,
                old_hours=args.old_hours,
                stale_hours=args.stale_hours,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_reply_context_staleness_table(report)
        if args.table or args.format == "table"
        else format_reply_context_staleness_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
