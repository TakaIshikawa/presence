#!/usr/bin/env python3
"""Recommend concrete upcoming publish windows for scheduling automation."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_window_recommender import (  # noqa: E402
    PublishWindowRecommendation,
    PublishWindowRecommender,
    recommendations_to_dicts,
)
from output.publish_caps import daily_platform_limits_from_config  # noqa: E402
from runner import script_context  # noqa: E402


def format_text_report(
    recommendations: list[PublishWindowRecommendation],
    *,
    platform: str,
    days: int,
    limit: int,
    content_type: str | None = None,
) -> str:
    """Format recommendations as a compact operator report."""
    content_label = f", content type: {content_type}" if content_type else ""
    lines = [
        "",
        "=" * 70,
        f"Publish Window Recommendations (next {days} days, platform: {platform}{content_label})",
        "=" * 70,
        "",
    ]
    if not recommendations:
        lines.append("No upcoming windows found from engagement history.")
        return "\n".join(lines)

    for index, item in enumerate(recommendations[:limit], 1):
        state = "available" if item.available else "unavailable"
        lines.append(
            f"{index}. {item.platform} at {item.start_time.isoformat()} "
            f"- score {item.score:.2f}, {state}"
        )
        for reason in item.reasons:
            lines.append(f"   - {reason}")
    return "\n".join(lines)


def format_json_report(recommendations: list[PublishWindowRecommendation]) -> str:
    """Format recommendations as stable JSON."""
    return json.dumps(recommendations_to_dicts(recommendations), indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rank upcoming publish windows by engagement history and daily caps"
    )
    parser.add_argument(
        "--platform",
        default="all",
        choices=["all", "x", "bluesky"],
        help="Platform to recommend for (default: all)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to look ahead (default: 7)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of recommendations to return (default: 10)",
    )
    parser.add_argument(
        "--content-type",
        help="Optional generated_content.content_type filter for engagement history",
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
        recommender = PublishWindowRecommender(
            db,
            daily_limits=daily_platform_limits_from_config(config),
        )
        recommendations = recommender.recommend(
            platform=args.platform,
            days=args.days,
            limit=args.limit,
            content_type=args.content_type,
        )

    if args.json:
        print(format_json_report(recommendations))
    else:
        print(
            format_text_report(
                recommendations,
                platform=args.platform,
                days=args.days,
                limit=args.limit,
                content_type=args.content_type,
            )
        )


if __name__ == "__main__":
    main()
