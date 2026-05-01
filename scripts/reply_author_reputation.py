#!/usr/bin/env python3
"""Score repeat inbound reply authors by reputation."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_author_reputation import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_INTERACTIONS,
    build_reply_author_reputation_report,
    format_reply_author_reputation_json,
    format_reply_author_reputation_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back by interaction time (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-interactions",
        type=int,
        default=DEFAULT_MIN_INTERACTIONS,
        help=(
            "Minimum reply_queue interactions required per author "
            f"(default: {DEFAULT_MIN_INTERACTIONS})."
        ),
    )
    parser.add_argument(
        "--platform",
        help="Restrict reputation scoring to one reply platform.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum authors to include (default: {DEFAULT_LIMIT}).",
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
            report = build_reply_author_reputation_report(
                db,
                days=args.days,
                min_interactions=args.min_interactions,
                platform=args.platform,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_author_reputation_json(report))
    else:
        print(format_reply_author_reputation_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
