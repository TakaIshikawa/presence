#!/usr/bin/env python3
"""Report published posts whose engagement momentum has flattened."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.engagement_decay import (  # noqa: E402
    EngagementDecayAnalyzer,
    format_engagement_decay_json,
    format_engagement_decay_table,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        default="all",
        choices=["all", "x", "twitter", "bluesky", "bsky"],
        help="Restrict to one platform (default: all)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Look back this many days (default: 14)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum rows to print",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = EngagementDecayAnalyzer(db).analyze(
            days=args.days,
            platform=args.platform,
            limit=args.limit,
        )

    if args.json:
        print(format_engagement_decay_json(report))
    else:
        print(format_engagement_decay_table(report))


if __name__ == "__main__":
    main()
