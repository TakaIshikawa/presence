#!/usr/bin/env python3
"""Evaluate recorded model usage against spend budgets."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.model_budget_guard import (  # noqa: E402
    evaluate_model_budget,
    format_text_report,
)
from runner import script_context  # noqa: E402


def parse_operation_budget(value: str) -> tuple[str, float]:
    """Parse an operation budget argument in operation=amount form."""
    if "=" not in value:
        raise argparse.ArgumentTypeError(
            "--operation-budget must use operation=amount"
        )
    operation, amount = value.split("=", 1)
    operation = operation.strip()
    if not operation:
        raise argparse.ArgumentTypeError("operation name is required")
    try:
        budget = float(amount)
    except ValueError:
        raise argparse.ArgumentTypeError("operation budget must be a number") from None
    if budget < 0:
        raise argparse.ArgumentTypeError("operation budget must be non-negative")
    return operation, budget


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--monthly-budget",
        type=float,
        default=None,
        help="Projected monthly total budget in USD",
    )
    parser.add_argument(
        "--operation-budget",
        action="append",
        type=parse_operation_budget,
        default=[],
        metavar="operation=amount",
        help="Projected monthly budget for an operation; repeatable",
    )
    parser.add_argument(
        "--format",
        default="text",
        choices=["text", "json"],
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Exit nonzero when any budget warning is produced",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.days < 1:
        parser.error("--days must be at least 1")
    if args.monthly_budget is not None and args.monthly_budget < 0:
        parser.error("--monthly-budget must be non-negative")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    operation_budgets = dict(args.operation_budget)
    with script_context() as (_config, db):
        report = evaluate_model_budget(
            db,
            days=args.days,
            monthly_budget=args.monthly_budget,
            operation_budgets=operation_budgets,
        )

    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(format_text_report(report))

    return 1 if args.fail_on_warning and report.warnings else 0


if __name__ == "__main__":
    raise SystemExit(main())
