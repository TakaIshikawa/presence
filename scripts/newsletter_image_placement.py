#!/usr/bin/env python3
"""Report newsletter image placement and density issues."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_image_placement import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MAX_IMAGES_PER_SECTION,
    build_newsletter_image_placement_report,
    build_newsletter_image_placement_report_from_text,
    format_newsletter_image_placement_json,
    format_newsletter_image_placement_text,
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
        help=f"Lookback window in days by newsletter timestamp (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum newsletter rows to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--max-images-per-section",
        type=_positive_int,
        default=DEFAULT_MAX_IMAGES_PER_SECTION,
        help=(
            "Flag sections above this image count "
            f"(default: {DEFAULT_MAX_IMAGES_PER_SECTION})."
        ),
    )
    parser.add_argument(
        "--input",
        help="Analyze a newsletter body from this file instead of the database; use '-' for stdin.",
    )
    parser.add_argument(
        "--subject",
        default="",
        help="Optional subject label for --input reports.",
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

        if args.input:
            body = (
                sys.stdin.read()
                if args.input == "-"
                else Path(args.input).read_text(encoding="utf-8")
            )
            report = build_newsletter_image_placement_report_from_text(
                body,
                newsletter_id=args.input,
                subject=args.subject,
                max_images_per_section=args.max_images_per_section,
            )
        else:
            with script_context() as (_config, db):
                report = build_newsletter_image_placement_report(
                    db,
                    days=args.days,
                    limit=args.limit,
                    max_images_per_section=args.max_images_per_section,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_image_placement_json(report))
    else:
        print(format_newsletter_image_placement_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
