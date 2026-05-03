#!/usr/bin/env python3
"""Report draft newsletter CTA coverage."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_cta_coverage import (  # noqa: E402
    COVERAGE_STATUSES,
    DEFAULT_LIMIT,
    build_newsletter_cta_coverage_report,
    format_newsletter_cta_coverage_json,
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
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum draft newsletter rows to inspect (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--coverage",
        action="append",
        choices=COVERAGE_STATUSES,
        help="Only emit rows with this CTA coverage status; may be passed more than once.",
    )
    parser.add_argument(
        "--weak-or-missing",
        action="store_true",
        help="Only emit rows with weak or missing CTA coverage.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    coverage = tuple(args.coverage or ())
    if args.weak_or_missing:
        coverage = tuple(dict.fromkeys((*coverage, "weak", "missing")))

    try:
        with script_context() as (_config, db):
            report = build_newsletter_cta_coverage_report(
                db,
                limit=args.limit,
                coverage_filter=coverage,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_newsletter_cta_coverage_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
