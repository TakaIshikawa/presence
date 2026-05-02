#!/usr/bin/env python3
"""Report pending reply drafts that do not answer inbound questions."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_answer_coverage import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STATUS,
    build_reply_answer_coverage_report,
    format_reply_answer_coverage_json,
    format_reply_answer_coverage_text,
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
        "--status",
        default=DEFAULT_STATUS,
        help=(
            f"Only include drafts with this status (default: {DEFAULT_STATUS}). "
            "Use 'all' for every status."
        ),
    )
    parser.add_argument("--platform", help="Only include drafts for this platform.")
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum reply_queue rows to scan after filters (default: {DEFAULT_LIMIT}).",
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
    status = None if args.status == "all" else args.status
    try:
        with script_context() as (_config, db):
            report = build_reply_answer_coverage_report(
                db,
                status=status,
                platform=args.platform,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_answer_coverage_json(report))
    else:
        print(format_reply_answer_coverage_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
