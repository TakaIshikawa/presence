#!/usr/bin/env python3
"""Report repeated newsletter subject-line patterns before they fatigue readers."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_subject_fatigue import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_THRESHOLD,
    build_newsletter_subject_fatigue_report,
    format_newsletter_subject_fatigue_json,
    format_newsletter_subject_fatigue_text,
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
        help=f"Lookback window in days for subject candidates (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--threshold",
        type=_positive_int,
        default=DEFAULT_THRESHOLD,
        help=(
            "Minimum repeated opening or punctuation uses to report "
            f"(default: {DEFAULT_THRESHOLD})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_newsletter_subject_fatigue_report(
                db,
                days=args.days,
                threshold=args.threshold,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_subject_fatigue_json(report))
    else:
        print(format_newsletter_subject_fatigue_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
