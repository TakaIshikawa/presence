#!/usr/bin/env python3
"""Export newsletter seed sections from ingested GitHub releases."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.release_newsletter_seed import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_BODY_LENGTH,
    build_release_newsletter_seed_report,
    format_release_newsletter_seed_json,
    format_release_newsletter_seed_text,
)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be zero or positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for release activity (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument("--repo", help="Only include releases for this repo name.")
    parser.add_argument(
        "--min-body-length",
        type=_non_negative_int,
        default=DEFAULT_MIN_BODY_LENGTH,
        help=(
            "Minimum normalized release body length before a seed is exported "
            f"(default: {DEFAULT_MIN_BODY_LENGTH})."
        ),
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
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_release_newsletter_seed_report(
                    conn,
                    days=args.days,
                    repo=args.repo,
                    min_body_length=args.min_body_length,
                )
        else:
            with script_context() as (_config, db):
                report = build_release_newsletter_seed_report(
                    db,
                    days=args.days,
                    repo=args.repo,
                    min_body_length=args.min_body_length,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_release_newsletter_seed_json(report))
    else:
        print(format_release_newsletter_seed_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
