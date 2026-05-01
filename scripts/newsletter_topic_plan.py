#!/usr/bin/env python3
"""Recommend topics for the next newsletter."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_topic_planner import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    NewsletterTopicRecommendation,
    build_newsletter_topic_plan,
    format_newsletter_topic_plan_json,
)
from runner import script_context  # noqa: E402


def format_newsletter_topic_plan_table(
    recommendations: list[NewsletterTopicRecommendation],
    *,
    days: int,
) -> str:
    lines = [
        f"Newsletter Topic Plan (last {days} days)",
        f"recommendations={len(recommendations)}",
        "",
        f"{'Topic':22s}  {'Type':16s}  {'Use':>3s}  {'Content':>7s}  {'Planned':>7s}  {'Fresh':>5s}  Reason",
        f"{'-' * 22:22s}  {'-' * 16:16s}  {'-' * 3:>3s}  {'-' * 7:>7s}  {'-' * 7:>7s}  {'-' * 5:>5s}  {'-' * 40}",
    ]
    if not recommendations:
        lines.append("No recommendations found.")
        return "\n".join(lines)

    for item in recommendations:
        freshness = "-" if item.freshness_days is None else f"{item.freshness_days}d"
        lines.append(
            f"{_clip(item.topic, 22):22s}  "
            f"{item.recommendation_type:16s}  "
            f"{item.recent_newsletter_uses:3d}  "
            f"{item.available_content_count:7d}  "
            f"{item.open_planned_topic_count:7d}  "
            f"{freshness:>5s}  "
            f"{_clip(item.reason, 80)}"
        )
        support = _format_support(item)
        if support:
            lines.append(f"{'':22s}  support: {support}")
    return "\n".join(lines)


def _format_support(item: NewsletterTopicRecommendation) -> str:
    parts = []
    if item.supporting_content_ids:
        parts.append(
            "content="
            + ",".join(str(content_id) for content_id in item.supporting_content_ids)
        )
    if item.supporting_planned_topic_ids:
        parts.append(
            "planned="
            + ",".join(str(topic_id) for topic_id in item.supporting_planned_topic_ids)
        )
    if item.campaign_names:
        parts.append("campaigns=" + ", ".join(item.campaign_names))
    return "; ".join(parts)


def _clip(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Lookback window for newsletter sends and content (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum recommendations to return (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--include-planned",
        action="store_true",
        help="Include open planned_topics as recommendation inventory",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        recommendations = build_newsletter_topic_plan(
            db,
            days=args.days,
            limit=args.limit,
            include_planned=args.include_planned,
        )

    output = (
        format_newsletter_topic_plan_json(recommendations)
        if args.json
        else format_newsletter_topic_plan_table(recommendations, days=args.days)
    )
    print(output)


if __name__ == "__main__":
    main()
