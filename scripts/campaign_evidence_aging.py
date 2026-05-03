#!/usr/bin/env python3
"""Report campaigns whose linked evidence is old or sparse."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_evidence_aging import (  # noqa: E402
    DEFAULT_MAX_AGE_DAYS,
    STATUSES,
    build_campaign_evidence_aging_report,
    format_campaign_evidence_aging_json,
    format_campaign_evidence_aging_text,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


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
        "--max-age-days",
        type=_non_negative_int,
        default=DEFAULT_MAX_AGE_DAYS,
        help=f"Maximum acceptable linked evidence age in days (default: {DEFAULT_MAX_AGE_DAYS}).",
    )
    parser.add_argument(
        "--campaign-id",
        type=_positive_int,
        help="Restrict to one campaign id.",
    )
    parser.add_argument(
        "--status",
        choices=STATUSES,
        help="Restrict to campaigns with one evidence freshness status.",
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
            report = build_campaign_evidence_aging_report(
                db,
                max_age_days=args.max_age_days,
                campaign_id=args.campaign_id,
                status=args.status,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_campaign_evidence_aging_json(report))
    else:
        print(format_campaign_evidence_aging_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
