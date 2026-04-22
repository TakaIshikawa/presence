#!/usr/bin/env python3
"""Summarize recorded Anthropic model usage and estimated cost."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context

logger = logging.getLogger(__name__)


def _fmt_cost(value: float | None) -> str:
    return f"${float(value or 0):.4f}"


def format_text_report(rows: list[dict], days: int) -> str:
    """Format model usage summary as a human-readable report."""
    if not rows:
        return f"No model usage found in last {days} days."

    total_calls = sum(int(row.get("call_count") or 0) for row in rows)
    total_tokens = sum(int(row.get("total_tokens") or 0) for row in rows)
    total_cost = sum(float(row.get("estimated_cost") or 0) for row in rows)

    lines = [
        "",
        "=" * 78,
        f"Model Usage Report (last {days} days)",
        "=" * 78,
        "",
        f"Total calls:  {total_calls}",
        f"Total tokens: {total_tokens}",
        f"Total cost:   {_fmt_cost(total_cost)}",
        "",
        f"{'Day':10s} {'Operation':36s} {'Model':22s} {'Calls':>5s} {'Tokens':>8s} {'Cost':>9s}",
        f"{'-' * 10:10s} {'-' * 36:36s} {'-' * 22:22s} {'-' * 5:>5s} {'-' * 8:>8s} {'-' * 9:>9s}",
    ]
    for row in rows:
        operation = str(row.get("operation_name") or "")[:36]
        model = str(row.get("model_name") or "")[:22]
        lines.append(
            f"{str(row.get('day') or ''):10s} "
            f"{operation:36s} "
            f"{model:22s} "
            f"{int(row.get('call_count') or 0):5d} "
            f"{int(row.get('total_tokens') or 0):8d} "
            f"{_fmt_cost(row.get('estimated_cost')):>9s}"
        )
    lines.extend(["", "=" * 78, ""])
    return "\n".join(lines)


def format_json_report(rows: list[dict], days: int) -> str:
    """Format model usage summary as JSON."""
    totals = {
        "call_count": sum(int(row.get("call_count") or 0) for row in rows),
        "input_tokens": sum(int(row.get("input_tokens") or 0) for row in rows),
        "output_tokens": sum(int(row.get("output_tokens") or 0) for row in rows),
        "total_tokens": sum(int(row.get("total_tokens") or 0) for row in rows),
        "estimated_cost": round(
            sum(float(row.get("estimated_cost") or 0) for row in rows), 6
        ),
    }
    return json.dumps({"days": days, "totals": totals, "rows": rows}, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
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

    with script_context() as (_config, db):
        rows = db.get_model_usage_summary(since_days=args.days)
        if args.format == "json":
            print(format_json_report(rows, args.days))
        else:
            print(format_text_report(rows, args.days))


if __name__ == "__main__":
    main()
