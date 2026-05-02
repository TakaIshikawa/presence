#!/usr/bin/env python3
"""Seed content ideas from GitHub security advisory activity."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.security_advisory_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    format_security_advisory_ideas_json,
    format_security_advisory_ideas_text,
    seed_security_advisory_ideas,
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
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for security advisories (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument("--repo", help="Only include one repo, for example owner/name.")
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum advisories to process (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--include-withdrawn",
        action="store_true",
        help="Include withdrawn advisories. Default skips them.",
    )
    parser.add_argument(
        "--insert",
        action="store_true",
        help="Insert eligible content_ideas rows. Default is dry-run preview.",
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
        with script_context() as (_config, db):
            report = seed_security_advisory_ideas(
                db,
                days=args.days,
                repo=args.repo,
                limit=args.limit,
                include_withdrawn=args.include_withdrawn,
                dry_run=not args.insert,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_security_advisory_ideas_json(report))
    else:
        print(format_security_advisory_ideas_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
