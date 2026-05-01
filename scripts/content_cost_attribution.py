#!/usr/bin/env python3
"""Report model spend attributed to generated content."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_cost_attribution import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_COST,
    ContentCostAttribution,
    export_to_json,
    format_text_report,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back by model usage time (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--content-type",
        default=None,
        help="Filter to one generated_content.content_type",
    )
    parser.add_argument(
        "--published",
        nargs="?",
        const="published",
        default="all",
        choices=(
            "all",
            "published",
            "unpublished",
            "yes",
            "no",
            "true",
            "false",
            "1",
            "0",
        ),
        help="Filter publication state; omit value to include only published content",
    )
    parser.add_argument(
        "--min-cost",
        type=float,
        default=DEFAULT_MIN_COST,
        help=f"Minimum attributed estimated cost (default: {DEFAULT_MIN_COST:g})",
    )
    parser.add_argument("--json", action="store_true", help="Print stable JSON output")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum content items to include (default: {DEFAULT_LIMIT})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    with script_context() as (_config, db):
        report = ContentCostAttribution(db).build_report(
            days=args.days,
            content_type=args.content_type,
            published=args.published,
            min_cost=args.min_cost,
            limit=args.limit,
        )

    if args.json:
        print(export_to_json(report))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
