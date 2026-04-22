#!/usr/bin/env python3
"""Generate campaign-level content performance reports."""

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.pipeline_analytics import CampaignPerformanceReport, PipelineAnalytics
from runner import script_context


def _content_preview(content: str, max_len: int = 90) -> str:
    """Return a single-line content preview for terminal output."""
    preview = " ".join((content or "").split())
    if len(preview) <= max_len:
        return preview
    return f"{preview[: max_len - 3]}..."


def format_text_report(report: CampaignPerformanceReport | None) -> str:
    """Format a campaign report for humans."""
    if report is None:
        return "No campaign data found."

    campaign = report.campaign
    lines = [
        "",
        "=" * 70,
        f"Campaign Report: {campaign['name']}",
        "=" * 70,
        "",
        f"Campaign ID: {campaign['id']}",
        f"Status:      {campaign['status']}",
        f"Window:      {campaign.get('start_date') or 'n/a'} to {campaign.get('end_date') or 'n/a'}",
        f"Lookback:    last {report.period_days} days",
    ]
    if campaign.get("goal"):
        lines.append(f"Goal:        {campaign['goal']}")
    lines.append("")

    counts = report.topic_counts
    lines.append("Topics:")
    lines.append(
        f"  Total {counts.get('total', 0)}, "
        f"planned {counts.get('planned', 0)}, "
        f"generated {counts.get('generated', 0)}, "
        f"skipped {counts.get('skipped', 0)}"
    )
    lines.append(f"  Avg eval score: {report.avg_eval_score:.2f}/10")
    lines.append("")

    lines.append("Platform Engagement:")
    for platform, stats in report.per_platform_engagement.items():
        if platform == "newsletter":
            lines.append(
                "  newsletter: "
                f"{stats['send_count']} sends, "
                f"{stats['content_count']} content items, "
                f"{stats['subscriber_count_total']} subscriber impressions"
            )
        else:
            lines.append(
                f"  {platform}: "
                f"{stats['content_count']} content items, "
                f"avg {stats['avg_engagement_score']:.2f}, "
                f"total {stats['total_engagement_score']:.2f}"
            )
    lines.append("")

    if report.top_content:
        lines.append("Top Content:")
        for item in report.top_content:
            lines.append(
                f"  #{item['content_id']} {item['topic']} "
                f"(eval {item['eval_score'] if item['eval_score'] is not None else 'n/a'}, "
                f"eng {item['combined_engagement_score']:.2f})"
            )
            lines.append(f"    {_content_preview(item['content'])}")
        lines.append("")

    if report.gaps:
        lines.append("Gaps:")
        for gap in report.gaps:
            label = gap["type"].replace("_", " ")
            target = gap.get("target_date") or "n/a"
            content = f" content #{gap['content_id']}" if gap.get("content_id") else ""
            lines.append(f"  {label}: {gap.get('topic', 'n/a')} ({target}){content}")
        lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)


def format_json_report(report: CampaignPerformanceReport | None) -> str:
    """Format a campaign report as JSON."""
    if report is None:
        return json.dumps({"error": "No campaign data found"}, indent=2)

    data = asdict(report)
    data["period_start"] = report.period_start.isoformat()
    data["period_end"] = report.period_end.isoformat()
    return json.dumps(data, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report campaign topic completion and content performance"
    )
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Content campaign ID to report",
    )
    parser.add_argument(
        "--active",
        action="store_true",
        help="Report the currently active campaign",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Engagement/newsletter lookback in days (default: 30)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    args = parser.parse_args()

    if args.campaign_id is not None and args.active:
        parser.error("--campaign-id and --active are mutually exclusive")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        analytics = PipelineAnalytics(db)
        report = analytics.campaign_performance_report(
            campaign_id=args.campaign_id,
            active=args.active or args.campaign_id is None,
            days=args.days,
        )
        if args.json:
            print(format_json_report(report))
        else:
            print(format_text_report(report))


if __name__ == "__main__":
    main()
