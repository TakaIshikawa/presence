#!/usr/bin/env python3
"""Report generated content rows missing topic coverage."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.generated_content_topic_coverage import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_CONFIDENCE,
    build_generated_content_topic_coverage_report,
    format_generated_content_topic_coverage_json,
    format_generated_content_topic_coverage_text,
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


def _confidence(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid confidence: {value}") from exc
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("confidence must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for generated_content rows (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--content-type",
        help="Restrict to one generated_content.content_type.",
    )
    parser.add_argument(
        "--published-only",
        action="store_true",
        help="Restrict to generated content that appears published.",
    )
    parser.add_argument(
        "--min-confidence",
        type=_confidence,
        default=DEFAULT_MIN_CONFIDENCE,
        help=(
            "Minimum acceptable content_topics.confidence, 0-1 "
            f"(default: {DEFAULT_MIN_CONFIDENCE})."
        ),
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
        help="Exit with status 1 when topic coverage issues are found.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_generated_content_topic_coverage_report(
                db,
                days=args.days,
                content_type=args.content_type,
                published_only=args.published_only,
                min_confidence=args.min_confidence,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_generated_content_topic_coverage_json(report))
    else:
        print(format_generated_content_topic_coverage_text(report))
    if args.fail_on_issues and report.has_issues:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
