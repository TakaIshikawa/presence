#!/usr/bin/env python3
"""Recommend prompt rollback when current versions underperform baselines."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.prompt_rollback import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_SAMPLES,
    build_prompt_rollback_report,
    format_prompt_rollback_json,
    format_prompt_rollback_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prompt-type",
        help="Restrict analysis to one prompt type.",
    )
    parser.add_argument(
        "--lookback-days",
        "--days",
        dest="days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=DEFAULT_MIN_SAMPLES,
        help=(
            "Minimum evidence rows needed for current and baseline versions "
            f"before recommending rollback (default: {DEFAULT_MIN_SAMPLES})."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print deterministic JSON instead of the default text report.",
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
            report = build_prompt_rollback_report(
                db,
                prompt_type=args.prompt_type,
                days=args.days,
                min_samples=args.min_samples,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_prompt_rollback_json(report))
    else:
        print(format_prompt_rollback_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
