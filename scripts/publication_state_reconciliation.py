#!/usr/bin/env python3
"""Report platform publication state drift across publication tables."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_state_reconciliation import (  # noqa: E402
    DEFAULT_DAYS,
    build_publication_state_reconciliation_report,
    format_publication_state_reconciliation_json,
    format_publication_state_reconciliation_text,
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
        help=f"Publication state lookback in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        action="append",
        help="Platform to reconcile. Repeat for multiple platforms. Defaults to all platforms.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit 1 when reconciliation issues are found.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_publication_state_reconciliation_report(
                db,
                days=args.days,
                platforms=tuple(args.platform or ()),
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publication_state_reconciliation_json(report))
    else:
        print(format_publication_state_reconciliation_text(report))
    return 1 if args.fail_on_issues and report.has_issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
