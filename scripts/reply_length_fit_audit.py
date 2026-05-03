#!/usr/bin/env python3
"""Audit reply drafts against platform length budgets."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_length_fit_audit import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_NEAR_THRESHOLD,
    build_reply_length_fit_audit,
    format_reply_length_fit_audit_json,
    format_reply_length_fit_audit_text,
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


def _near_threshold(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if not 0 < parsed <= 1:
        raise argparse.ArgumentTypeError("value must be greater than 0 and at most 1")
    return parsed


def _platform_limit(value: str) -> tuple[str, int]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("value must be PLATFORM=LIMIT")
    platform, raw_limit = value.split("=", 1)
    platform = platform.strip().lower()
    if not platform:
        raise argparse.ArgumentTypeError("platform must not be blank")
    limit = _positive_int(raw_limit)
    return platform, limit


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--platform-limit",
        action="append",
        type=_platform_limit,
        default=[],
        metavar="PLATFORM=LIMIT",
        help="Override a platform length limit; repeat for multiple platforms.",
    )
    parser.add_argument(
        "--near-threshold",
        type=_near_threshold,
        default=DEFAULT_NEAR_THRESHOLD,
        help=f"Fraction of limit considered near-limit (default: {DEFAULT_NEAR_THRESHOLD:g}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum reply_queue rows to audit (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    platform_limits = dict(args.platform_limit)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_length_fit_audit(
                    conn,
                    platform_limits=platform_limits,
                    near_threshold=args.near_threshold,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_reply_length_fit_audit(
                    db,
                    platform_limits=platform_limits,
                    near_threshold=args.near_threshold,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_length_fit_audit_json(report))
    else:
        print(format_reply_length_fit_audit_text(report))
    return 1 if report.blocking_issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
