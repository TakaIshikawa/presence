#!/usr/bin/env python3
"""Report recurring reply_queue quality flag and low-score issues."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_quality_flag_digest import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MAX_SCORE,
    build_reply_quality_flag_digest_report,
    format_reply_quality_flag_digest_json,
    format_reply_quality_flag_digest_text,
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


def _score(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid score: {value}") from exc
    if parsed < 0 or parsed > 10:
        raise argparse.ArgumentTypeError("score must be between 0 and 10")
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
        "--status",
        action="append",
        default=[],
        help=(
            "Reply status to include. Repeat for multiple statuses "
            "(default: pending, approved, dismissed)."
        ),
    )
    parser.add_argument(
        "--max-score",
        type=_score,
        default=DEFAULT_MAX_SCORE,
        help=(
            "Include scored rows below this quality_score as actionable "
            f"(default: {DEFAULT_MAX_SCORE:.1f})."
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
        help="Exit 2 when malformed flags or actionable replies are found.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_reply_quality_flag_digest_report(
                db,
                days=args.days,
                statuses=tuple(args.status) if args.status else None,
                max_score=args.max_score,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_quality_flag_digest_json(report))
    else:
        print(format_reply_quality_flag_digest_text(report))
    if args.fail_on_issues and report.get("has_issues"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
