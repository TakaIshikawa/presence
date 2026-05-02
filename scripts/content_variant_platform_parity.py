#!/usr/bin/env python3
"""Report generated content missing expected platform variants."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_variant_platform_parity import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_PLATFORMS,
    DEFAULT_STALE_THRESHOLD_DAYS,
    build_content_variant_platform_parity_report,
    format_content_variant_platform_parity_json,
    format_content_variant_platform_parity_text,
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


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days by generated_content.created_at (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        action="append",
        dest="platforms",
        help=(
            "Expected platform to check. Repeat for multiple platforms "
            f"(default: {', '.join(DEFAULT_PLATFORMS)})."
        ),
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
        help=f"Maximum content items to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--stale-threshold-days",
        type=_non_negative_int,
        default=DEFAULT_STALE_THRESHOLD_DAYS,
        help=(
            "Grace period before a variant older than the source edit timestamp "
            f"is stale (default: {DEFAULT_STALE_THRESHOLD_DAYS})."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_content_variant_platform_parity_report(
                db,
                days=args.days,
                platforms=args.platforms,
                limit=args.limit,
                stale_threshold_days=args.stale_threshold_days,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_variant_platform_parity_json(report))
    else:
        print(format_content_variant_platform_parity_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
