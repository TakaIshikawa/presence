#!/usr/bin/env python3
"""Check configured model usage budgets against recorded usage."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from model_usage import ModelUsageBudgetSummary, summarize_model_usage_budget
from runner import script_context


def _fmt_cost(value: float | None) -> str:
    return "not configured" if value is None else f"${value:.4f}"


def format_text_summary(summary: ModelUsageBudgetSummary) -> str:
    status = "exceeded" if summary.exceeded else "within budget"
    lines = [
        f"Model usage budget ({summary.period})",
        f"Status:    {status}",
        f"Spend:     {_fmt_cost(summary.spend)}",
        f"Limit:     {_fmt_cost(summary.limit)}",
        f"Remaining: {_fmt_cost(summary.remaining)}",
        f"Window:    {summary.start_at} to {summary.end_at}",
    ]
    return "\n".join(lines)


def format_json_summary(summary: ModelUsageBudgetSummary) -> str:
    return json.dumps(asdict(summary), indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--period",
        choices=["daily", "monthly"],
        default="daily",
        help="Budget period to check (default: daily)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON",
    )
    parser.add_argument(
        "--fail-on-exceeded",
        action="store_true",
        help="Exit nonzero when the configured budget is exceeded",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        summary = summarize_model_usage_budget(
            db,
            period=args.period,
            daily_limit=config.model_usage.max_daily_estimated_cost,
            monthly_limit=config.model_usage.max_monthly_estimated_cost,
        )

    print(format_json_summary(summary) if args.json else format_text_summary(summary))
    if args.fail_on_exceeded and summary.exceeded:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
