#!/usr/bin/env python3
"""Report newsletter subscriber growth and churn momentum."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_subscriber_momentum import (  # noqa: E402
    DEFAULT_CHURN_WARNING_RATE,
    DEFAULT_DAYS,
    build_newsletter_subscriber_momentum_report,
    format_newsletter_subscriber_momentum_json,
    format_newsletter_subscriber_momentum_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back by snapshot time (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--churn-warning-rate",
        type=float,
        default=DEFAULT_CHURN_WARNING_RATE,
        help=(
            "Warn when average churn rate exceeds this decimal rate "
            f"(default: {DEFAULT_CHURN_WARNING_RATE:g})"
        ),
    )
    parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="text",
        help="Output format (default: text)",
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
            report = build_newsletter_subscriber_momentum_report(
                db,
                days=args.days,
                churn_warning_rate=args.churn_warning_rate,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_subscriber_momentum_json(report))
    else:
        print(format_newsletter_subscriber_momentum_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
