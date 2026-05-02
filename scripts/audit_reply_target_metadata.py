#!/usr/bin/env python3
"""Audit pending reply drafts for incomplete target metadata."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_target_metadata_audit import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_reply_target_metadata_audit,
    format_reply_target_metadata_audit_json,
    format_reply_target_metadata_audit_text,
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
        help=f"Lookback window in days for pending reply drafts (default: {DEFAULT_DAYS}).",
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
        help=f"Maximum pending reply drafts to audit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic JSON instead of compact text.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_reply_target_metadata_audit(
                db,
                days=args.days,
                platform=tuple(args.platform or ()),
                limit=args.limit,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_reply_target_metadata_audit_json(report))
    else:
        print(format_reply_target_metadata_audit_text(report))
    if report.blocking_issue_count:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
