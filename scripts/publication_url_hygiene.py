#!/usr/bin/env python3
"""Audit published generated content for publication URL hygiene issues."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_url_hygiene import (  # noqa: E402
    DEFAULT_DAYS,
    ISSUE_TYPES,
    build_publication_url_hygiene_report,
    format_publication_url_hygiene_json,
    format_publication_url_hygiene_text,
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
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--platform")
    parser.add_argument("--issue-type", choices=sorted(ISSUE_TYPES))
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_publication_url_hygiene_report(
                db,
                days=args.days,
                platform=args.platform,
                issue_type=args.issue_type,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_publication_url_hygiene_json(report)
        if args.format == "json"
        else format_publication_url_hygiene_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
