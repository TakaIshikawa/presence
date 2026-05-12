#!/usr/bin/env python3
"""Audit generated content variant selection health."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_variant_selection_audit import (  # noqa: E402
    DEFAULT_DAYS,
    build_content_variant_selection_audit_report,
    format_content_variant_selection_audit_json,
    format_content_variant_selection_audit_text,
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
        help=f"Only inspect generated content from this many days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument("--platform", help="Only inspect this content_variants platform.")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit with status 1 when audit issues are found.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_content_variant_selection_audit_report(
                db,
                days=args.days,
                platform=args.platform,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(format_content_variant_selection_audit_json(report))
    else:
        print(format_content_variant_selection_audit_text(report))
    if args.fail_on_issues and report.totals["issues_found"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
