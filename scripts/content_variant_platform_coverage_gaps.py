#!/usr/bin/env python3
"""Report generated content missing expected platform variant rows."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_variant_platform_coverage_gaps import (  # noqa: E402
    DEFAULT_EXPECTED_MATRIX,
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_DAYS,
    build_content_variant_platform_coverage_gaps_report_from_db,
    format_content_variant_platform_coverage_gaps_json,
    format_content_variant_platform_coverage_gaps_text,
    parse_expected_pair,
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


def _expected_pair(value: str) -> tuple[str, str]:
    try:
        return parse_expected_pair(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument(
        "--expect",
        action="append",
        type=_expected_pair,
        dest="expected_matrix",
        help=(
            "Expected platform:variant_type pair. Repeat for multiple pairs "
            f"(default: {', '.join(f'{platform}:{variant_type}' for platform, variant_type in DEFAULT_EXPECTED_MATRIX)})."
        ),
    )
    parser.add_argument("--content-type", help="Only include generated_content rows with this content_type.")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    kwargs = {
        "content_type": args.content_type,
        "expected_matrix": args.expected_matrix,
        "limit": args.limit,
        "lookback_days": args.lookback_days,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_content_variant_platform_coverage_gaps_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_content_variant_platform_coverage_gaps_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_content_variant_platform_coverage_gaps_text(report)
        if args.format == "text"
        else format_content_variant_platform_coverage_gaps_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
