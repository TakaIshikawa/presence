#!/usr/bin/env python3
"""Report repeated CTA families across campaign-generated content."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_cta_rotation import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_REPEAT,
    build_campaign_cta_rotation_report,
    format_campaign_cta_rotation_json,
    format_campaign_cta_rotation_text,
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
        help=f"Generated content lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--campaign-id",
        type=_positive_int,
        help="Only audit generated content linked to this campaign.",
    )
    parser.add_argument(
        "--min-repeat",
        type=_positive_int,
        default=DEFAULT_MIN_REPEAT,
        help=f"CTA family count that should be flagged (default: {DEFAULT_MIN_REPEAT}).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of the default text report.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_campaign_cta_rotation_report(
                db,
                days=args.days,
                campaign_id=args.campaign_id,
                min_repeat=args.min_repeat,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_campaign_cta_rotation_json(report))
    else:
        print(format_campaign_cta_rotation_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
