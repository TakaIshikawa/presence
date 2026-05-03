#!/usr/bin/env python3
"""Audit queued reply drafts for tone consistency."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_tone_consistency_audit import (  # noqa: E402
    DEFAULT_BASELINE_STATUSES,
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_BASELINE,
    DEFAULT_QUEUED_STATUSES,
    build_reply_tone_consistency_audit,
    format_reply_tone_consistency_audit_json,
    format_reply_tone_consistency_audit_markdown,
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
        help=f"Lookback window in days for reply drafts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-baseline",
        type=_positive_int,
        default=DEFAULT_MIN_BASELINE,
        help=(
            "Minimum approved/published replies required before auditing "
            f"(default: {DEFAULT_MIN_BASELINE})."
        ),
    )
    parser.add_argument(
        "--status",
        action="append",
        dest="queued_statuses",
        help=(
            "Queued reply status to audit. Repeat for multiple statuses "
            f"(default: {', '.join(DEFAULT_QUEUED_STATUSES)})."
        ),
    )
    parser.add_argument(
        "--baseline-status",
        action="append",
        dest="baseline_statuses",
        help=(
            "Approved/published status to include in the tone baseline. Repeat for multiple "
            f"statuses (default: {', '.join(DEFAULT_BASELINE_STATUSES)})."
        ),
    )
    parser.add_argument(
        "--platform",
        action="append",
        help="Platform to audit. Repeat for multiple platforms. Defaults to all platforms.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum findings to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="Output format (default: markdown).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_reply_tone_consistency_audit(
                db,
                days=args.days,
                min_baseline=args.min_baseline,
                queued_statuses=tuple(args.queued_statuses or ()),
                baseline_statuses=tuple(args.baseline_statuses or ()),
                platform=tuple(args.platform or ()),
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_tone_consistency_audit_json(report))
    else:
        print(format_reply_tone_consistency_audit_markdown(report))
    return 1 if report.blocking_issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
