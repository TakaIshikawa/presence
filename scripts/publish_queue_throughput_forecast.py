#!/usr/bin/env python3
"""Forecast publish queue backlog clearance from recent throughput."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_queue_throughput_forecast import (  # noqa: E402
    DEFAULT_HORIZON_DAYS,
    DEFAULT_LOOKBACK_DAYS,
    build_publish_queue_throughput_forecast,
    format_publish_queue_throughput_forecast_json,
    format_publish_queue_throughput_forecast_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=(
            "Recent successful publication window to use for throughput "
            f"(default: {DEFAULT_LOOKBACK_DAYS})."
        ),
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=DEFAULT_HORIZON_DAYS,
        help=(
            "Clearance horizon used for recommendation codes "
            f"(default: {DEFAULT_HORIZON_DAYS})."
        ),
    )
    parser.add_argument(
        "--platform",
        default="all",
        help="Restrict forecast to a queue platform value (default: all).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
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
            forecast = build_publish_queue_throughput_forecast(
                db,
                lookback_days=args.lookback_days,
                horizon_days=args.horizon_days,
                platform=args.platform,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publish_queue_throughput_forecast_json(forecast))
    else:
        print(format_publish_queue_throughput_forecast_text(forecast))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
