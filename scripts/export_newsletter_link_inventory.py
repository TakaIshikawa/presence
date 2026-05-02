#!/usr/bin/env python3
"""Export newsletter outbound link inventory."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_link_inventory import (  # noqa: E402
    DEFAULT_RECENT_COUNT,
    build_newsletter_link_inventory_report,
    format_newsletter_link_inventory_json,
    format_newsletter_link_inventory_text,
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
    parser.add_argument(
        "--newsletter-id",
        action="append",
        default=[],
        help=(
            "Newsletter send id, issue id, or generated newsletter content id. "
            "Repeat for multiple newsletters."
        ),
    )
    parser.add_argument(
        "--recent-count",
        type=_positive_int,
        help=(
            "Export this many most recent newsletter sends/content rows "
            f"(default when no ids are supplied: {DEFAULT_RECENT_COUNT})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="json",
        help="Output format (default: json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.newsletter_id and args.recent_count is not None:
            raise ValueError("--recent-count cannot be combined with --newsletter-id")
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_newsletter_link_inventory_report(
                    conn,
                    newsletter_ids=tuple(args.newsletter_id),
                    recent_count=args.recent_count,
                )
        else:
            with script_context() as (_config, db):
                report = build_newsletter_link_inventory_report(
                    db,
                    newsletter_ids=tuple(args.newsletter_id),
                    recent_count=args.recent_count,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "text":
        print(format_newsletter_link_inventory_text(report))
    else:
        print(format_newsletter_link_inventory_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
