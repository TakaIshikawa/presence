#!/usr/bin/env python3
"""Mine recurring reply FAQ clusters and optionally seed content ideas."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_faq_miner import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_COUNT,
    build_reply_faq_miner,
    format_reply_faq_miner_json,
    format_reply_faq_miner_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=DEFAULT_MIN_COUNT,
        help=f"Minimum replies needed for a cluster (default: {DEFAULT_MIN_COUNT})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum clusters to return (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Create content_ideas for new clusters.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
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
            report = build_reply_faq_miner(
                db,
                days=args.days,
                min_count=args.min_count,
                limit=args.limit,
                apply=args.apply,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_faq_miner_json(report))
    else:
        print(format_reply_faq_miner_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
