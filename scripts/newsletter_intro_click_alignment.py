#!/usr/bin/env python3
"""Report whether newsletter introductions match clicked links."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_intro_click_alignment import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_CLICKS,
    build_newsletter_intro_click_alignment_report,
    format_newsletter_intro_click_alignment_json,
    format_newsletter_intro_click_alignment_text,
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
        help=f"Lookback window in days by newsletter send timestamp (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum ranked sent newsletters to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--min-clicks",
        type=_positive_int,
        default=DEFAULT_MIN_CLICKS,
        help=(
            "Minimum clicked links per send to include in ranking "
            f"(default: {DEFAULT_MIN_CLICKS})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)
        with script_context() as (_config, db):
            report = build_newsletter_intro_click_alignment_report(
                db,
                days=args.days,
                limit=args.limit,
                min_clicks=args.min_clicks,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_intro_click_alignment_json(report))
    else:
        print(format_newsletter_intro_click_alignment_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
