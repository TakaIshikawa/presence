#!/usr/bin/env python3
"""Plan or apply retry policy for failed publication attempts."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.retry_policy import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MAX_ATTEMPTS,
    build_retry_policy_plan,
    format_retry_policy_plan_json,
    format_retry_policy_plan_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        choices=["all", "x", "bluesky"],
        default="all",
        help="Platform to include (default: all)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back by failed attempt time (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
        help=f"Mark failures terminal at this attempt count (default: {DEFAULT_MAX_ATTEMPTS})",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply proposed retry times and terminal cancellation updates",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
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
            plan = build_retry_policy_plan(
                db,
                platform=args.platform,
                days=args.days,
                max_attempts=args.max_attempts,
                apply=args.apply,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_retry_policy_plan_json(plan))
    else:
        print(format_retry_policy_plan_text(plan))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
