#!/usr/bin/env python3
"""Plan candidate blog series from related generated content."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.blog_series_planner import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_ITEMS,
    build_blog_series_plan,
    format_blog_series_plan_json,
    format_blog_series_plan_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Recent activity window to consider (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-items",
        type=int,
        default=DEFAULT_MIN_ITEMS,
        help=f"Minimum content IDs required for a series (default: {DEFAULT_MIN_ITEMS}).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output deterministic JSON instead of text.",
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
            plan = build_blog_series_plan(
                db,
                days=args.days,
                min_items=args.min_items,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_blog_series_plan_json(plan))
    else:
        print(format_blog_series_plan_text(plan))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
