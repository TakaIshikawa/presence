#!/usr/bin/env python3
"""Measure GitHub activity conversion into generated and published content."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.github_activity_conversion import (  # noqa: E402
    DEFAULT_DAYS,
    build_github_activity_conversion_report,
    format_github_activity_conversion_json,
    format_github_activity_conversion_text,
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
    parser.add_argument("--repository")
    parser.add_argument("--activity-type")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_github_activity_conversion_report(
                db,
                days=args.days,
                repository=args.repository,
                activity_type=args.activity_type,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_github_activity_conversion_json(report)
        if args.format == "json"
        else format_github_activity_conversion_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
