#!/usr/bin/env python3
"""Report newsletter source freshness and reuse."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_source_freshness import (  # noqa: E402
    DEFAULT_MAX_SOURCE_AGE_DAYS,
    DEFAULT_REUSE_THRESHOLD,
    build_newsletter_source_freshness_report,
    format_newsletter_source_freshness_json,
    format_newsletter_source_freshness_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-source-age-days", type=_positive_int, default=DEFAULT_MAX_SOURCE_AGE_DAYS)
    parser.add_argument("--reuse-threshold", type=_positive_int, default=DEFAULT_REUSE_THRESHOLD)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_newsletter_source_freshness_report(
                db,
                max_source_age_days=args.max_source_age_days,
                reuse_threshold=args.reuse_threshold,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.json or args.format == "json":
        print(format_newsletter_source_freshness_json(report))
    else:
        print(format_newsletter_source_freshness_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
