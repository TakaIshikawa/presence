#!/usr/bin/env python3
"""Report knowledge rows whose license metadata conflicts with curated sources."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_license_conflicts import (  # noqa: E402
    DEFAULT_LIMIT,
    LICENSE_ALL,
    build_source_license_conflict_report,
    format_source_license_conflict_json,
    format_source_license_conflict_text,
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
        "--license",
        default=LICENSE_ALL,
        help="Curated source license to audit: open, attribution_required, restricted, or all.",
    )
    parser.add_argument(
        "--source-type",
        help="Only audit one curated source type, such as x_account, blog, or newsletter.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum findings to emit (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_source_license_conflict_report(
                db,
                license_filter=args.license,
                source_type=args.source_type,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_source_license_conflict_json(report))
    else:
        print(format_source_license_conflict_text(report))
    return 1 if report.finding_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
