#!/usr/bin/env python3
"""Report repeat authors across reply and proactive engagement queues."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.repeat_author_insights import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_COUNT,
    DEFAULT_STALE_DAYS,
    build_repeat_author_insights_report,
    format_repeat_author_insights_json,
    format_repeat_author_insights_text,
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
        "--min-count",
        type=int,
        default=DEFAULT_MIN_COUNT,
        help=(
            "Minimum appearances required to classify an author as active "
            f"(default: {DEFAULT_MIN_COUNT})."
        ),
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=DEFAULT_STALE_DAYS,
        help=f"Classify authors as stale after this many days idle (default: {DEFAULT_STALE_DAYS}).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
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
            report = build_repeat_author_insights_report(
                db,
                days=args.days,
                min_count=args.min_count,
                stale_days=args.stale_days,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_repeat_author_insights_json(report))
    else:
        print(format_repeat_author_insights_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
