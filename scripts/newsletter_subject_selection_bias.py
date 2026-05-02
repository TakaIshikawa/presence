#!/usr/bin/env python3
"""Report newsletter subject selection bias across generated candidates."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_subject_selection_bias import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_CANDIDATES_PER_ISSUE,
    build_newsletter_subject_selection_bias_report,
    format_newsletter_subject_selection_bias_json,
    format_newsletter_subject_selection_bias_text,
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
        help=f"Lookback window in days for subject selections (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-candidates-per-issue",
        type=_positive_int,
        default=DEFAULT_MIN_CANDIDATES_PER_ISSUE,
        help=(
            "Minimum candidates in an issue/send pool before it is included "
            f"(default: {DEFAULT_MIN_CANDIDATES_PER_ISSUE})."
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
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_newsletter_subject_selection_bias_report(
                db,
                days=args.days,
                min_candidates_per_issue=args.min_candidates_per_issue,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_subject_selection_bias_json(report))
    else:
        print(format_newsletter_subject_selection_bias_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
