#!/usr/bin/env python3
"""Build a concise campaign postmortem brief for operator review."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_postmortem_brief import (  # noqa: E402
    DEFAULT_DAYS,
    build_campaign_postmortem_brief,
    format_json_brief,
    format_markdown_brief,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign-id",
        type=int,
        required=True,
        help="content_campaigns.id to review.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="Output format (default: markdown).",
    )
    parser.add_argument(
        "--include-posts",
        action="store_true",
        help="Include per-post rows in the brief.",
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
            brief = build_campaign_postmortem_brief(
                db,
                campaign_id=args.campaign_id,
                days=args.days,
                include_posts=args.include_posts,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_json_brief(brief))
    else:
        print(format_markdown_brief(brief))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
