#!/usr/bin/env python3
"""Report reply draft intent mix across pending and reviewed queues."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_intent_mix import (  # noqa: E402
    DEFAULT_DAYS,
    build_reply_intent_mix_report,
    format_reply_intent_mix_json,
    format_reply_intent_mix_text,
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
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Recent lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument("--platform", help="Only include drafts for this platform.")
    parser.add_argument(
        "--pending-only",
        action="store_true",
        help="Exclude reviewed, dismissed, and sent reply drafts.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_reply_intent_mix_report(
                db,
                days=args.days,
                platform=args.platform,
                include_reviewed=not args.pending_only,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_intent_mix_json(report))
    else:
        print(format_reply_intent_mix_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
