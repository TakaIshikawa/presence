#!/usr/bin/env python3
"""Measure citation density in generated content."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.citation_density import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MAX_PER_100,
    DEFAULT_MIN_PER_100,
    build_citation_density_report,
    format_citation_density_json,
    format_citation_density_text,
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


def _nonnegative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_float(value: str) -> float:
    parsed = _nonnegative_float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--content-type")
    parser.add_argument("--min-per-100", type=_nonnegative_float, default=DEFAULT_MIN_PER_100)
    parser.add_argument("--max-per-100", type=_positive_float, default=DEFAULT_MAX_PER_100)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_citation_density_report(
                db,
                days=args.days,
                content_type=args.content_type,
                min_per_100=args.min_per_100,
                max_per_100=args.max_per_100,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_citation_density_json(report)
        if args.format == "json"
        else format_citation_density_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
