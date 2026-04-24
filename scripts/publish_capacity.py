#!/usr/bin/env python3
"""Forecast publish queue capacity against daily caps and posting windows."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_capacity import (
    PublishCapacityForecast,
    forecast_publish_capacity,
)
from runner import script_context


def _parse_now(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid ISO timestamp: {value}"
        ) from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Forecast horizon in days (default: 7)",
    )
    parser.add_argument(
        "--platform",
        choices=["all", "x", "bluesky"],
        default="all",
        help="Platform to forecast (default: all)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    parser.add_argument(
        "--now",
        type=_parse_now,
        help="Deterministic ISO timestamp override for tests",
    )
    return parser.parse_args(argv)


def format_json_report(forecast: PublishCapacityForecast) -> str:
    """Format a capacity forecast as stable JSON."""
    return json.dumps(forecast.as_dict(), indent=2, sort_keys=True)


def format_text_report(forecast: PublishCapacityForecast) -> str:
    """Format a capacity forecast for operator review."""
    lines = [
        "",
        "=" * 70,
        f"Publish Capacity Forecast ({forecast.horizon_days} days)",
        "=" * 70,
        f"generated_at: {forecast.generated_at}",
        f"horizon_days: {forecast.horizon_days}",
        "",
    ]

    if not forecast.platforms:
        lines.append("No platforms selected.")
        return "\n".join(lines)

    for platform in forecast.platforms:
        lines.append(f"platform: {platform.platform}")
        lines.append(f"  queued_count: {platform.queued_count}")
        lines.append(
            f"  projected_publish_slots: {len(platform.projected_publish_slots)}"
        )
        if platform.projected_publish_slots:
            for slot in platform.projected_publish_slots:
                lines.append(f"    - {slot}")
        lines.append(f"  overflow_count: {platform.overflow_count}")
        lines.append(
            "  estimated_clearance_time: "
            f"{platform.estimated_clearance_time or '-'}"
        )
        lines.append("")

    return "\n".join(lines).rstrip()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        forecast = forecast_publish_capacity(
            db,
            config,
            days=args.days,
            platform=args.platform,
            now=args.now,
        )

    if args.json:
        print(format_json_report(forecast))
    else:
        print(format_text_report(forecast))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
