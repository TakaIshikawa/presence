#!/usr/bin/env python3
"""Report failed and borderline persona guard rows for operator review."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.persona_guard_failure_digest import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_persona_guard_failure_digest,
    format_persona_guard_failure_digest_json,
    format_persona_guard_failure_digest_text,
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


def _score(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid score: {value}") from exc
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("score must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Persona guard lookback in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-score",
        type=_score,
        default=None,
        help="Include passing checked rows below this score.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum rows to output (default: {DEFAULT_LIMIT}).",
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
            report = build_persona_guard_failure_digest(
                db,
                days=args.days,
                min_score=args.min_score,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_persona_guard_failure_digest_json(report))
    else:
        print(format_persona_guard_failure_digest_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
