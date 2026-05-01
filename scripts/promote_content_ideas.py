#!/usr/bin/env python3
"""Report open content ideas that are strong promotion candidates."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.content_idea_promotion import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_content_idea_promotion_report,
    format_content_idea_promotion_json,
    format_content_idea_promotion_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days of engagement snapshots to consider (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum candidates to return (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--include-snoozed",
        action="store_true",
        help="Include open ideas that are currently snoozed.",
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
            report = build_content_idea_promotion_report(
                db,
                days=args.days,
                limit=args.limit,
                include_snoozed=args.include_snoozed,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_idea_promotion_json(report))
    else:
        print(format_content_idea_promotion_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
