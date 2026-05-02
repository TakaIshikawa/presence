#!/usr/bin/env python3
"""Check queued reply drafts for citation sufficiency."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_citation_sufficiency import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STATUS,
    build_reply_citation_sufficiency_report,
    format_reply_citation_sufficiency_json,
    format_reply_citation_sufficiency_text,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--status",
        default=DEFAULT_STATUS,
        help=(
            "Comma-separated reply statuses to inspect, or 'all' "
            f"(default: {DEFAULT_STATUS})."
        ),
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum reply drafts to inspect; 0 means no limit (default: {DEFAULT_LIMIT}).",
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
    logging.basicConfig(
        level=logging.WARNING if args.format == "json" else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        with script_context() as (_config, db):
            report = build_reply_citation_sufficiency_report(
                db,
                status=args.status,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_citation_sufficiency_json(report))
    else:
        print(format_reply_citation_sufficiency_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
