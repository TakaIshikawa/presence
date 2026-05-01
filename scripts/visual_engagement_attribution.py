#!/usr/bin/env python3
"""Report engagement deltas attributable to visual asset usage."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.visual_engagement_attribution import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_SAMPLE,
    build_visual_engagement_attribution_report,
    format_visual_engagement_attribution_json,
    format_visual_engagement_attribution_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        help="Only include one publication platform, e.g. x, bluesky, linkedin, or mastodon.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--min-sample",
        type=int,
        default=DEFAULT_MIN_SAMPLE,
        help=f"Minimum visual and non-visual sample count for attribution (default: {DEFAULT_MIN_SAMPLE}).",
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
            report = build_visual_engagement_attribution_report(
                db,
                days=args.days,
                platform=args.platform,
                min_sample=args.min_sample,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_visual_engagement_attribution_json(report))
    else:
        print(format_visual_engagement_attribution_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
