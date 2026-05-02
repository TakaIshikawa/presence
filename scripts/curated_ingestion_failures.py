#!/usr/bin/env python3
"""Report curated source ingestion failures as JSON."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.curated_ingestion_failure_digest import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_FAILURES,
    build_curated_ingestion_failure_digest_report,
    format_curated_ingestion_failure_digest_json,
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
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days by failure timestamp (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-failures",
        type=_positive_int,
        default=DEFAULT_MIN_FAILURES,
        help=f"Minimum consecutive failures to include (default: {DEFAULT_MIN_FAILURES}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)

        with script_context() as (_config, db):
            report = build_curated_ingestion_failure_digest_report(
                db,
                days=args.days,
                min_failures=args.min_failures,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_curated_ingestion_failure_digest_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
