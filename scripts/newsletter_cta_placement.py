#!/usr/bin/env python3
"""Report newsletter CTA placement problems."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_cta_placement import (  # noqa: E402
    DEFAULT_CTA_MARKER_PATTERNS,
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_PARAGRAPH_THRESHOLD,
    build_newsletter_cta_placement_report,
    format_newsletter_cta_placement_json,
    format_newsletter_cta_placement_text,
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
        help=f"Maximum newsletter rows to inspect (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--paragraph-threshold",
        type=_positive_int,
        default=DEFAULT_PARAGRAPH_THRESHOLD,
        help=(
            "Flag drafts whose first CTA appears after this paragraph "
            f"(default: {DEFAULT_PARAGRAPH_THRESHOLD})."
        ),
    )
    parser.add_argument(
        "--cta-marker",
        action="append",
        dest="cta_markers",
        help=(
            "Regex pattern used to detect CTA text. Can be passed more than once; "
            "defaults to built-in newsletter CTA markers."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic sorted-key JSON.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    patterns = tuple(args.cta_markers or DEFAULT_CTA_MARKER_PATTERNS)
    try:
        with script_context() as (_config, db):
            report = build_newsletter_cta_placement_report(
                db,
                days=args.days,
                limit=args.limit,
                paragraph_threshold=args.paragraph_threshold,
                cta_marker_patterns=patterns,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_newsletter_cta_placement_json(report))
    else:
        print(format_newsletter_cta_placement_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
