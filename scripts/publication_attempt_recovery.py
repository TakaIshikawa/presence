#!/usr/bin/env python3
"""Report whether publication errors recover after retry."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_attempt_recovery import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_REPRESENTATIVE_LIMIT,
    PLATFORMS,
    build_publication_attempt_recovery_report,
    format_publication_attempt_recovery_json,
    format_publication_attempt_recovery_text,
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
        help=f"Number of days to look back for failed attempts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        choices=("all", *PLATFORMS),
        default="all",
        help="Platform to include (default: all).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--representative-limit",
        type=_positive_int,
        default=DEFAULT_REPRESENTATIVE_LIMIT,
        help=(
            "Maximum representative content IDs per bucket "
            f"(default: {DEFAULT_REPRESENTATIVE_LIMIT})."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_publication_attempt_recovery_report(
                db,
                days=args.days,
                platform=args.platform,
                representative_limit=args.representative_limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publication_attempt_recovery_json(report))
    else:
        print(format_publication_attempt_recovery_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
