#!/usr/bin/env python3
"""Report recurring media publication failure patterns."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_media_failure_patterns import (  # noqa: E402
    DEFAULT_LIMIT,
    build_publication_media_failure_patterns_report_from_db,
    format_publication_media_failure_patterns_json,
    format_publication_media_failure_patterns_text,
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
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Print the human-readable table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_publication_media_failure_patterns_report_from_db(db, limit=args.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_text = args.table or args.format == "text"
    print(
        format_publication_media_failure_patterns_text(report)
        if as_text
        else format_publication_media_failure_patterns_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
