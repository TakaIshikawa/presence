#!/usr/bin/env python3
"""Report author and domain diversity in linked knowledge usage."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.author_diversity import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_USAGE,
    DEFAULT_STALE_AFTER_DAYS,
    DEFAULT_TOP_N,
    build_knowledge_author_diversity_report,
    format_knowledge_author_diversity_json,
    format_knowledge_author_diversity_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days",
        "--days",
        dest="days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Number of author/domain buckets to show (default: {DEFAULT_TOP_N}).",
    )
    parser.add_argument(
        "--min-usage",
        type=int,
        default=DEFAULT_MIN_USAGE,
        help=(
            "Minimum linked knowledge uses needed before concentration warnings "
            f"are actionable (default: {DEFAULT_MIN_USAGE})."
        ),
    )
    parser.add_argument(
        "--stale-after-days",
        type=int,
        default=DEFAULT_STALE_AFTER_DAYS,
        help=(
            "Knowledge source age threshold for stale usage "
            f"(default: {DEFAULT_STALE_AFTER_DAYS})."
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
            report = build_knowledge_author_diversity_report(
                db,
                days=args.days,
                top_n=args.top_n,
                min_usage=args.min_usage,
                stale_after_days=args.stale_after_days,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_knowledge_author_diversity_json(report))
    else:
        print(format_knowledge_author_diversity_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
