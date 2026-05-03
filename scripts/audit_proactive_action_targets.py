#!/usr/bin/env python3
"""Audit proactive actions for duplicate or conflicting engagement targets."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.proactive_action_target_audit import (  # noqa: E402
    DEFAULT_DAYS,
    VALID_ACTION_TYPES,
    build_proactive_action_target_audit,
    format_proactive_action_target_audit_json,
    format_proactive_action_target_audit_text,
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


def _action_type(value: str) -> str:
    parsed = value.strip().casefold()
    if parsed not in VALID_ACTION_TYPES:
        raise argparse.ArgumentTypeError(
            "action_type must be one of: " + ", ".join(sorted(VALID_ACTION_TYPES))
        )
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days by proactive action timestamp (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--action-type",
        action="append",
        type=_action_type,
        help="Action type to include. Repeat for multiple action types. Defaults to all action types.",
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
        help="Exit with status 1 when duplicate or conflicting target issues are found.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_proactive_action_target_audit(
                db,
                days=args.days,
                action_types=tuple(args.action_type or ()),
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_proactive_action_target_audit_json(report))
    else:
        print(format_proactive_action_target_audit_text(report))
    return 1 if args.fail_on_issues and report.blocking_issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
