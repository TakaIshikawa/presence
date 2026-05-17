#!/usr/bin/env python3
"""Report reply draft tone drift from approved or published baselines."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_tone_consistency import (  # noqa: E402
    DEFAULT_BASELINE_LIMIT,
    DEFAULT_DRAFT_LIMIT,
    build_reply_tone_consistency_report_from_db,
    format_reply_tone_consistency_json,
    format_reply_tone_consistency_table,
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
    parser.add_argument("--baseline-limit", type=_positive_int, default=DEFAULT_BASELINE_LIMIT)
    parser.add_argument("--draft-limit", type=_positive_int, default=DEFAULT_DRAFT_LIMIT)
    parser.add_argument("--format", choices=("json", "table", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_reply_tone_consistency_report_from_db(
                db,
                baseline_limit=args.baseline_limit,
                draft_limit=args.draft_limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_reply_tone_consistency_json(report) if args.format == "json" else format_reply_tone_consistency_table(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
