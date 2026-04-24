#!/usr/bin/env python3
"""Forecast model usage cost against configured synthesis budgets."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from evaluation.cost_forecast import CostForecast, forecast_from_db
from storage.db import Database

logger = logging.getLogger(__name__)


def _fmt_cost(value: float | None) -> str:
    return "unlimited" if value is None else f"${float(value):.4f}"


def format_json_report(forecast: CostForecast) -> str:
    """Format the forecast as stable JSON."""
    return json.dumps(forecast.to_dict(), indent=2, sort_keys=True)


def format_text_report(forecast: CostForecast) -> str:
    """Format the forecast as a human-readable report."""
    lines = [
        "",
        "=" * 78,
        f"Model Cost Forecast ({forecast.lookback_days}-day history)",
        "=" * 78,
        "",
        f"Status:              {forecast.status}",
        f"Today spend:         {_fmt_cost(forecast.today_spend)}",
        f"Daily budget:        {_fmt_cost(forecast.daily_budget)}",
        f"Remaining today:     {_fmt_cost(forecast.remaining_daily_budget)}",
        f"Per-run budget:      {_fmt_cost(forecast.per_run_budget)}",
        f"Avg recent run cost: {_fmt_cost(forecast.average_recent_run_cost)}",
        "Safe runs today:     "
        + (
            "unlimited"
            if forecast.safe_run_count_today is None
            else str(forecast.safe_run_count_today)
        ),
        "",
        forecast.message,
    ]

    if forecast.content_types:
        lines.extend(
            [
                "",
                "By content type:",
                f"{'Content type':16s} {'Runs':>5s} {'Avg/run':>10s} {'Safe':>8s} {'Status':>11s}",
                f"{'-' * 16:16s} {'-' * 5:>5s} {'-' * 10:>10s} {'-' * 8:>8s} {'-' * 11:>11s}",
            ]
        )
        for item in forecast.content_types:
            safe = (
                "unlimited"
                if item.safe_run_count_today is None
                else str(item.safe_run_count_today)
            )
            lines.append(
                f"{item.content_type[:16]:16s} "
                f"{item.recent_run_count:5d} "
                f"{_fmt_cost(item.average_run_cost):>10s} "
                f"{safe:>8s} "
                f"{item.status:>11s}"
            )

    if forecast.operations:
        lines.extend(
            [
                "",
                "By operation:",
                f"{'Content type':16s} {'Operation':38s} {'Calls':>5s} {'Avg/call':>10s}",
                f"{'-' * 16:16s} {'-' * 38:38s} {'-' * 5:>5s} {'-' * 10:>10s}",
            ]
        )
        for item in forecast.operations:
            lines.append(
                f"{item.content_type[:16]:16s} "
                f"{item.operation_name[:38]:38s} "
                f"{item.call_count:5d} "
                f"{_fmt_cost(item.average_call_cost):>10s}"
            )
    else:
        lines.extend(["", "No recent model usage history found."])

    lines.extend(["", "=" * 78, ""])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days of model usage history to use (default: 30)",
    )
    parser.add_argument(
        "--format",
        default="text",
        choices=["text", "json"],
        help="Output format (default: text)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = load_config()
    db = Database(config.paths.database)
    db.connect()
    try:
        forecast = forecast_from_db(
            db,
            max_estimated_cost_per_run=config.synthesis.max_estimated_cost_per_run,
            max_daily_estimated_cost=config.synthesis.max_daily_estimated_cost,
            lookback_days=args.days,
        )
    finally:
        db.close()

    if args.format == "json":
        print(format_json_report(forecast))
    else:
        print(format_text_report(forecast))


if __name__ == "__main__":
    main()
