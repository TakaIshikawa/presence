#!/usr/bin/env python3
"""Report recent newsletter sends that need churn triage."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_churn_triage import (  # noqa: E402
    DEFAULT_BASELINE_DAYS,
    DEFAULT_DAYS,
    DEFAULT_MIN_SENDS,
    build_newsletter_churn_triage_report,
    format_newsletter_churn_triage_json,
    format_newsletter_churn_triage_text,
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
        help=f"Recent send window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--baseline-days",
        type=_positive_int,
        default=DEFAULT_BASELINE_DAYS,
        help=f"Prior baseline window in days (default: {DEFAULT_BASELINE_DAYS}).",
    )
    parser.add_argument(
        "--min-sends",
        type=_positive_int,
        default=DEFAULT_MIN_SENDS,
        help=f"Minimum measured baseline sends (default: {DEFAULT_MIN_SENDS}).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print stable JSON instead of text.",
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
            report = build_newsletter_churn_triage_report(
                db,
                days=args.days,
                baseline_days=args.baseline_days,
                min_sends=args.min_sends,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_newsletter_churn_triage_json(report))
    else:
        print(format_newsletter_churn_triage_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
