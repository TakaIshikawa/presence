#!/usr/bin/env python3
"""Audit newsletter drafts for image references that can break after send."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_image_audit import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_STATUSES,
    build_newsletter_image_file_report,
    build_newsletter_image_queue_report,
    format_newsletter_image_audit_json,
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
        "--draft-file",
        help="Audit a local Markdown or HTML newsletter draft instead of queued records.",
    )
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Recent queued newsletter lookback in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--status",
        action="append",
        help=(
            "Queued newsletter status to include. Repeatable. "
            f"Defaults to: {', '.join(DEFAULT_STATUSES)}."
        ),
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum queued records to audit; 0 means no limit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--allow-relative",
        action="store_true",
        help="Do not flag relative image paths.",
    )
    parser.add_argument(
        "--fail-on-findings",
        action="store_true",
        help="Return exit code 2 when image findings are present.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        limit = None if args.limit == 0 else args.limit
        if args.draft_file:
            report = build_newsletter_image_file_report(
                Path(args.draft_file),
                allow_relative=args.allow_relative,
            )
        else:
            statuses = tuple(args.status) if args.status else DEFAULT_STATUSES
            with script_context() as (_config, db):
                report = build_newsletter_image_queue_report(
                    db,
                    days=args.days,
                    status=statuses,
                    limit=limit,
                    allow_relative=args.allow_relative,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_newsletter_image_audit_json(report))
    if args.fail_on_findings and report.totals["finding_count"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
