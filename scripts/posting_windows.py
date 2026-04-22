#!/usr/bin/env python3
"""Recommend posting windows from engagement history."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.posting_windows import PostingWindow, PostingWindowRecommender, windows_to_dicts
from runner import script_context


def format_text_report(
    windows: list[PostingWindow],
    days: int,
    platform: str,
    limit: int,
) -> str:
    """Format posting window recommendations as human-readable text."""
    lines = [
        "",
        "=" * 70,
        f"Posting Window Recommendations (last {days} days, platform: {platform})",
        "=" * 70,
        "",
    ]

    if not windows:
        lines.append("No published posts with engagement data found for this selection.")
        return "\n".join(lines)

    for index, window in enumerate(windows[:limit], 1):
        lines.append(
            f"{index}. {window.day_name} {window.hour_utc:02d}:00 UTC "
            f"- normalized {window.normalized_engagement:.2f}, "
            f"avg {window.avg_engagement:.2f}, "
            f"{window.sample_size} posts, "
            f"{window.confidence_label} confidence"
        )

    return "\n".join(lines)


def format_json_report(windows: list[PostingWindow]) -> str:
    """Format posting window recommendations as JSON."""
    return json.dumps(windows_to_dicts(windows), indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rank posting windows by historical X and Bluesky engagement"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to look back (default: 90)",
    )
    parser.add_argument(
        "--platform",
        default="all",
        choices=["all", "x", "bluesky"],
        help="Platform to analyze (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of windows to return (default: 10)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        recommender = PostingWindowRecommender(db)
        windows = recommender.recommend(
            days=args.days,
            platform=args.platform,
            limit=args.limit,
        )

    if args.json:
        print(format_json_report(windows))
    else:
        print(format_text_report(windows, args.days, args.platform, args.limit))


if __name__ == "__main__":
    main()
