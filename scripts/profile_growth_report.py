#!/usr/bin/env python3
"""Generate profile growth reports from profile_metrics."""

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.pipeline_analytics import PipelineAnalytics, ProfileGrowthReport
from runner import script_context


def format_delta(value: int | None) -> str:
    """Format an integer delta with a sign."""
    if value is None:
        return "n/a"
    return f"{value:+d}"


def format_pct(value: float | None) -> str:
    """Format a percentage delta."""
    if value is None:
        return "n/a"
    return f"{value:+.1f}%"


def format_text_report(report: ProfileGrowthReport) -> str:
    """Format a human-readable profile growth report."""
    lines = [
        "",
        "=" * 70,
        f"Profile Growth Report (last {report.period_days} days)",
        "=" * 70,
        "",
        f"Period: {report.period_start.strftime('%Y-%m-%d')} to {report.period_end.strftime('%Y-%m-%d')}",
        "",
    ]

    for platform, stats in report.platforms.items():
        lines.append(platform.upper())
        lines.append("-" * len(platform))
        lines.append(
            "Followers: "
            f"{stats.start_followers if stats.start_followers is not None else 'n/a'}"
            " -> "
            f"{stats.end_followers if stats.end_followers is not None else 'n/a'} "
            f"({format_delta(stats.follower_delta)}, {format_pct(stats.follower_delta_pct)})"
        )
        lines.append(
            "Following: "
            f"{stats.start_following if stats.start_following is not None else 'n/a'}"
            " -> "
            f"{stats.end_following if stats.end_following is not None else 'n/a'} "
            f"({format_delta(stats.following_delta)})"
        )
        lines.append(
            "Profile posts: "
            f"{stats.start_post_count if stats.start_post_count is not None else 'n/a'}"
            " -> "
            f"{stats.end_post_count if stats.end_post_count is not None else 'n/a'} "
            f"({format_delta(stats.profile_post_delta)})"
        )
        lines.append(f"Published volume: {stats.posting_volume}")
        lines.append(
            "Engagement score: "
            f"avg {stats.avg_engagement_score:.2f}, "
            f"min {stats.min_engagement_score:.2f}, "
            f"max {stats.max_engagement_score:.2f}, "
            f"total {stats.total_engagement_score:.2f} "
            f"({stats.engagement_count} posts)"
        )
        lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)


def format_json_report(report: ProfileGrowthReport) -> str:
    """Format profile growth report as JSON."""
    data = asdict(report)
    data["period_start"] = report.period_start.isoformat()
    data["period_end"] = report.period_end.isoformat()
    return json.dumps(data, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report profile follower deltas, posting volume, and engagement"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--platform",
        default="all",
        choices=["all", "x", "bluesky"],
        help="Platform to report (default: all)",
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
        analytics = PipelineAnalytics(db)
        report = analytics.profile_growth_report(
            days=args.days,
            platform=args.platform,
        )
        if args.json:
            print(format_json_report(report))
        else:
            print(format_text_report(report))


if __name__ == "__main__":
    main()
