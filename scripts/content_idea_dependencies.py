#!/usr/bin/env python3
"""Report content ideas that reference unresolved prerequisites."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.content_idea_dependencies import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STATUS,
    STATUSES,
    build_content_idea_dependency_report,
    format_content_idea_dependencies_json,
    format_content_idea_dependencies_text,
)


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
        "--status",
        choices=(*STATUSES, "all"),
        default=DEFAULT_STATUS,
        help=f"Content idea status to include, or 'all' (default: {DEFAULT_STATUS}).",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum content ideas to inspect (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    status = None if args.status == "all" else args.status
    try:
        with script_context() as (_config, db):
            rows = build_content_idea_dependency_report(
                db,
                status=status,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_idea_dependencies_json(rows))
    else:
        print(format_content_idea_dependencies_text(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
