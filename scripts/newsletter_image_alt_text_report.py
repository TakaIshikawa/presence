#!/usr/bin/env python3
"""Report newsletter image alt text coverage."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_image_alt_text_report import (  # noqa: E402
    DEFAULT_DAYS,
    build_newsletter_image_alt_text_report,
    format_newsletter_image_alt_text_csv,
    format_newsletter_image_alt_text_json,
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
        help=f"Newsletter lookback in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (default: json).",
    )
    parser.add_argument(
        "--output",
        help="Write the report to this path instead of stdout.",
    )
    parser.add_argument(
        "--include-descriptive",
        action="store_true",
        help="Include descriptive alt text rows; by default only actionable rows are emitted.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_newsletter_image_alt_text_report(
                db,
                days=args.days,
                include_descriptive=args.include_descriptive,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        rendered = format_newsletter_image_alt_text_csv(report)
    else:
        rendered = format_newsletter_image_alt_text_json(report)

    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
