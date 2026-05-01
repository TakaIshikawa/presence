#!/usr/bin/env python3
"""Report normalized engagement across social and newsletter platforms."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.cross_platform_engagement import (  # noqa: E402
    DEFAULT_DAYS,
    SUPPORTED_PLATFORMS,
    build_cross_platform_engagement_report,
    format_cross_platform_engagement_json,
    format_cross_platform_engagement_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back by fetched_at (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        choices=("all", *SUPPORTED_PLATFORMS),
        default="all",
        help="Restrict report to one platform (default: all).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output deterministic JSON instead of text.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum top/bottom rows to include (default: 10).",
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
            report = build_cross_platform_engagement_report(
                db,
                days=args.days,
                platform=args.platform,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_cross_platform_engagement_json(report))
    else:
        print(format_cross_platform_engagement_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
