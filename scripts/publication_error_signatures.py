#!/usr/bin/env python3
"""Report recurring publication attempt error signatures."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_error_signatures import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_COUNT,
    PLATFORMS,
    build_publication_error_signature_report,
    format_publication_error_signature_json,
    format_publication_error_signature_text,
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
        help=f"Look back this many days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        choices=("all", *PLATFORMS),
        default="all",
        help="Platform to include (default: all).",
    )
    parser.add_argument(
        "--min-count",
        type=_positive_int,
        default=DEFAULT_MIN_COUNT,
        help=f"Minimum failures per signature to report (default: {DEFAULT_MIN_COUNT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit 2 when recurring publication error signatures are found.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_publication_error_signature_report(
                db,
                days=args.days,
                platform=args.platform,
                min_count=args.min_count,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publication_error_signature_json(report))
    else:
        print(format_publication_error_signature_text(report))
    if args.fail_on_issues and report.to_dict().get("has_issues"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
