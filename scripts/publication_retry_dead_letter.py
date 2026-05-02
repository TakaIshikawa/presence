#!/usr/bin/env python3
"""Export publication retry dead-letter candidates."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_retry_dead_letter import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_FAILURES,
    DEFAULT_OLDER_THAN_HOURS,
    build_publication_retry_dead_letter_report,
    format_publication_retry_dead_letter_json,
    format_publication_retry_dead_letter_text,
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


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        help="SQLite database path. Defaults to configured database.",
    )
    parser.add_argument(
        "--min-failures",
        type=_positive_int,
        default=DEFAULT_MIN_FAILURES,
        help=(
            "Minimum failed attempts required for dead-letter candidacy "
            f"(default: {DEFAULT_MIN_FAILURES})."
        ),
    )
    parser.add_argument(
        "--older-than-hours",
        type=_positive_float,
        default=DEFAULT_OLDER_THAN_HOURS,
        help=(
            "Latest failure age required for dead-letter candidacy "
            f"(default: {DEFAULT_OLDER_THAN_HOURS})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum rows to output (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publication_retry_dead_letter_report(
                    conn,
                    min_failures=args.min_failures,
                    older_than_hours=args.older_than_hours,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_publication_retry_dead_letter_report(
                    db,
                    min_failures=args.min_failures,
                    older_than_hours=args.older_than_hours,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publication_retry_dead_letter_json(report))
    else:
        print(format_publication_retry_dead_letter_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
