#!/usr/bin/env python3
"""Forecast API rate limit exhaustion from stored snapshots."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.api_rate_limit_forecast import (  # noqa: E402
    DEFAULT_HOURS,
    DEFAULT_REMAINING_WARNING_PERCENT,
    build_api_rate_limit_forecast_report,
    format_api_rate_limit_forecast_json,
    format_api_rate_limit_forecast_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hours",
        type=int,
        default=DEFAULT_HOURS,
        help=f"Number of hours to look back by snapshot time (default: {DEFAULT_HOURS})",
    )
    parser.add_argument(
        "--remaining-warning-percent",
        type=float,
        default=DEFAULT_REMAINING_WARNING_PERCENT,
        help=(
            "Warn when remaining capacity is at or below this percent "
            f"(default: {DEFAULT_REMAINING_WARNING_PERCENT:g})"
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
            report = build_api_rate_limit_forecast_report(
                db,
                hours=args.hours,
                remaining_warning_percent=args.remaining_warning_percent,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_api_rate_limit_forecast_json(report))
    else:
        print(format_api_rate_limit_forecast_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
