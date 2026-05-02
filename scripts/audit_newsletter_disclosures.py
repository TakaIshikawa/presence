#!/usr/bin/env python3
"""Audit recent newsletters for sponsor language without disclosure."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_disclosure_audit import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_newsletter_disclosure_audit_report,
    format_newsletter_disclosure_audit_json,
    format_newsletter_disclosure_audit_text,
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


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Recent newsletter window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--status",
        action="append",
        default=[],
        help="Limit to one status. May be repeated or comma-separated.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic JSON instead of compact text.",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum newsletter rows to audit (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        with script_context() as (_config, db):
            report = build_newsletter_disclosure_audit_report(
                db,
                days=args.days,
                status=args.status,
                limit=args.limit,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_newsletter_disclosure_audit_json(report))
    else:
        print(format_newsletter_disclosure_audit_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
