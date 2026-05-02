#!/usr/bin/env python3
"""Audit generated X thread drafts for continuity before publishing."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.x_thread_continuity_audit import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_CHARS,
    DEFAULT_MIN_OVERLAP,
    build_x_thread_continuity_audit_report,
    format_x_thread_continuity_audit_json,
    format_x_thread_continuity_audit_text,
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


def _non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum findings to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--max-chars",
        type=_positive_int,
        default=DEFAULT_MAX_CHARS,
        help=f"Maximum X post length in characters (default: {DEFAULT_MAX_CHARS}).",
    )
    parser.add_argument(
        "--min-overlap",
        type=_non_negative_float,
        default=DEFAULT_MIN_OVERLAP,
        help=(
            "Minimum adjacent-post lexical overlap before flagging a transition "
            f"(default: {DEFAULT_MIN_OVERLAP})."
        ),
    )
    parser.add_argument(
        "--include-published",
        action="store_true",
        help="Also audit already-published generated threads.",
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
            report = build_x_thread_continuity_audit_report(
                db,
                limit=args.limit,
                max_chars=args.max_chars,
                min_overlap=args.min_overlap,
                include_published=args.include_published,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_x_thread_continuity_audit_json(report))
    else:
        print(format_x_thread_continuity_audit_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
