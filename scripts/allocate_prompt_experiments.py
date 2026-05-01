#!/usr/bin/env python3
"""Allocate future generation runs across prompt versions."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.prompt_experiment_allocator import (  # noqa: E402
    DEFAULT_EXPLORE_PERCENT,
    DEFAULT_MIN_RUNS,
    DEFAULT_TOTAL_RUNS,
    allocate_prompt_experiments,
    format_prompt_experiment_allocation_json,
    format_prompt_experiment_allocation_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prompt-type",
        help="Restrict allocation to one prompt type.",
    )
    parser.add_argument(
        "--total-runs",
        type=int,
        default=DEFAULT_TOTAL_RUNS,
        help=f"Future run budget to allocate (default: {DEFAULT_TOTAL_RUNS}).",
    )
    parser.add_argument(
        "--explore-percent",
        type=float,
        default=DEFAULT_EXPLORE_PERCENT,
        help=(
            "Percent of the run budget reserved for exploration "
            f"(default: {DEFAULT_EXPLORE_PERCENT})."
        ),
    )
    parser.add_argument(
        "--min-runs",
        type=int,
        default=DEFAULT_MIN_RUNS,
        help=(
            "Minimum historical runs before a prompt version is treated as sampled "
            f"(default: {DEFAULT_MIN_RUNS})."
        ),
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
            report = allocate_prompt_experiments(
                db,
                prompt_type=args.prompt_type,
                total_runs=args.total_runs,
                explore_percent=args.explore_percent,
                min_runs=args.min_runs,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_prompt_experiment_allocation_json(report))
    else:
        print(format_prompt_experiment_allocation_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
