#!/usr/bin/env python3
"""Audit open content ideas for missing or unusable evidence links."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.content_idea_evidence_links import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STATUS,
    PRIORITIES,
    STATUSES,
    build_content_idea_evidence_link_report,
    format_content_idea_evidence_link_json,
    format_content_idea_evidence_link_text,
)


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
        "--status",
        choices=(*STATUSES, "all"),
        default=DEFAULT_STATUS,
        help=f"Content idea status to include, or 'all' (default: {DEFAULT_STATUS}).",
    )
    parser.add_argument(
        "--priority",
        choices=PRIORITIES,
        help="Only include content ideas with this priority.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum findings to include (default: {DEFAULT_LIMIT}).",
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

    status = None if args.status == "all" else args.status
    try:
        with script_context() as (_config, db):
            report = build_content_idea_evidence_link_report(
                db,
                status=status,
                priority=args.priority,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_idea_evidence_link_json(report))
    else:
        print(format_content_idea_evidence_link_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
