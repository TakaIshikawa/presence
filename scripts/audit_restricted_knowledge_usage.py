#!/usr/bin/env python3
"""Audit unpublished generated content for restricted knowledge source usage."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.restricted_usage_audit import (  # noqa: E402
    DEFAULT_DAYS,
    LICENSE_ALL,
    LICENSE_ATTRIBUTION_REQUIRED,
    LICENSE_RESTRICTED,
    build_restricted_usage_audit_report,
    format_restricted_usage_audit_json,
    format_restricted_usage_audit_text,
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
        help=f"Recent lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--include-published",
        action="store_true",
        help="Include published generated content in addition to pre-publish content.",
    )
    parser.add_argument(
        "--license",
        choices=(LICENSE_RESTRICTED, LICENSE_ATTRIBUTION_REQUIRED, LICENSE_ALL),
        default=LICENSE_ALL,
        help="License class to audit (default: all).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_restricted_usage_audit_report(
                db,
                days=args.days,
                include_published=args.include_published,
                license_filter=args.license,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_restricted_usage_audit_json(report))
    else:
        print(format_restricted_usage_audit_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
