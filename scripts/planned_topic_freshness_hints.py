#!/usr/bin/env python3
"""Report source freshness hints for planned topics."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.planned_topic_freshness_hints import (  # noqa: E402
    DEFAULT_DAYS_STALE,
    build_planned_topic_freshness_hints_report,
    format_planned_topic_freshness_hints_json,
    format_planned_topic_freshness_hints_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days-stale",
        type=int,
        default=DEFAULT_DAYS_STALE,
        help=(
            "Mark source material stale after this many days "
            f"(default: {DEFAULT_DAYS_STALE})"
        ),
    )
    parser.add_argument(
        "--campaign",
        help="Content campaign ID or exact campaign name to report.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum planned topics to include.",
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
            report = build_planned_topic_freshness_hints_report(
                db,
                days_stale=args.days_stale,
                campaign=args.campaign,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_planned_topic_freshness_hints_json(report))
    else:
        print(format_planned_topic_freshness_hints_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
