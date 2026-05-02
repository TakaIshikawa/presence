#!/usr/bin/env python3
"""Plan stale proactive draft refresh work."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.proactive_draft_refresh import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STALE_DAYS,
    DEFAULT_STATUSES,
    VALID_ACTION_TYPES,
    VALID_STATUSES,
    build_proactive_draft_refresh_report,
    format_proactive_draft_refresh_json,
    format_proactive_draft_refresh_text,
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


def _choice(value: str, *, allowed: frozenset[str], name: str) -> str:
    parsed = value.strip().casefold()
    if parsed not in allowed:
        raise argparse.ArgumentTypeError(
            f"{name} must be one of: " + ", ".join(sorted(allowed))
        )
    return parsed


def _status(value: str) -> str:
    return _choice(value, allowed=VALID_STATUSES, name="status")


def _action_type(value: str) -> str:
    return _choice(value, allowed=VALID_ACTION_TYPES, name="action_type")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stale-days",
        type=_positive_int,
        default=DEFAULT_STALE_DAYS,
        help=f"Draft age threshold in days by created/reviewed anchor (default: {DEFAULT_STALE_DAYS}).",
    )
    parser.add_argument(
        "--status",
        action="append",
        type=_status,
        help=(
            "Proactive action status to include. Repeat for multiple statuses. "
            f"Defaults to: {', '.join(DEFAULT_STATUSES)}."
        ),
    )
    parser.add_argument(
        "--action-type",
        action="append",
        type=_action_type,
        help="Action type to include. Repeat for multiple action types. Defaults to all action types.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum proactive actions to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_proactive_draft_refresh_report(
                db,
                stale_days=args.stale_days,
                statuses=tuple(args.status or DEFAULT_STATUSES),
                action_types=tuple(args.action_type or ()),
                limit=args.limit,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_proactive_draft_refresh_json(report))
    else:
        print(format_proactive_draft_refresh_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
