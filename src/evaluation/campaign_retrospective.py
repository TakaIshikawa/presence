"""Campaign retrospective reporting."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from evaluation.pipeline_analytics import PipelineAnalytics
from storage.db import Database


def _content_preview(content: str | None, max_len: int = 120) -> str:
    preview = " ".join((content or "").split())
    if len(preview) <= max_len:
        return preview
    return f"{preview[: max_len - 3]}..."


class CampaignRetrospectiveGenerator:
    """Build after-action campaign reports from campaign analytics."""

    def __init__(self, db: Database) -> None:
        self.analytics = PipelineAnalytics(db)

    def build_report(
        self,
        campaign_id: int,
        top_limit: int = 5,
    ) -> dict[str, Any] | None:
        """Return a structured retrospective report for one campaign."""
        retrospective = self.analytics.campaign_retrospective_report(
            campaign_id=campaign_id,
            top_limit=top_limit,
        )
        if retrospective is None:
            return None

        topics = self.analytics._campaign_topic_rows(campaign_id)
        topic_counts = self.analytics._campaign_topic_counts(topics)
        report = asdict(retrospective)
        report["planned_topic_status_counts"] = topic_counts
        report["publication_metrics"] = self._publication_metrics(report)
        report["missed_topics"] = report.pop("missed_planned_topics")
        report["recommendations"] = self._recommendations(report)
        return report

    def _publication_metrics(self, report: dict[str, Any]) -> dict[str, Any]:
        platform_split = report["platform_split"]
        total_engagement = round(
            sum(
                stats.get("total_engagement_score", 0.0)
                for stats in platform_split.values()
            ),
            2,
        )
        engagement_count = sum(
            stats.get("engagement_count", 0) for stats in platform_split.values()
        )
        return {
            "published_items": report["published_items"],
            "platform_counts": {
                platform: stats.get("published_items", 0)
                for platform, stats in sorted(platform_split.items())
            },
            "platforms": platform_split,
            "total_engagement_score": total_engagement,
            "avg_engagement_score": report["avg_engagement_score"],
            "engagement_count": engagement_count,
        }

    def _recommendations(self, report: dict[str, Any]) -> list[str]:
        recommendations: list[str] = []
        planned = report["planned_topics"]
        generated = report["generated_topics"]
        published = report["published_items"]
        missed = report["missed_topics"]
        platform_split = report["platform_split"]
        top_content = report["top_content"]

        if missed:
            topic_names = ", ".join(topic["topic"] for topic in missed[:3])
            recommendations.append(
                f"Schedule or intentionally skip missed planned topics: {topic_names}."
            )
        if generated and published == 0:
            recommendations.append(
                "Move generated campaign content into the publish queue before adding new topics."
            )
        if planned and generated < planned:
            recommendations.append(
                "Keep the next campaign plan smaller or add generation slots until planned topics are caught up."
            )
        if published and not any(
            stats.get("engagement_count", 0) for stats in platform_split.values()
        ):
            recommendations.append(
                "Fetch engagement for published campaign posts so the next retrospective can rank outcomes."
            )
        if top_content:
            best = top_content[0]
            recommendations.append(
                f"Continue the strongest angle, {best['topic']}, and repurpose content #{best['content_id']}."
            )
        if not published and not generated:
            recommendations.append(
                "Start with one concrete planned topic and publish a small test post before expanding the campaign."
            )
        if not recommendations:
            recommendations.append(
                "Close the campaign or create the next plan using the top-performing platform and topic mix."
            )
        return recommendations


def report_to_dict(report: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize report output for JSON serialization."""
    if report is None:
        return {"error": "No campaign data found"}
    return report


def format_json_report(report: dict[str, Any] | None) -> str:
    """Format a retrospective report as stable JSON."""
    return json.dumps(report_to_dict(report), indent=2, sort_keys=True)


def format_markdown_report(report: dict[str, Any] | None) -> str:
    """Format a retrospective report as markdown."""
    if report is None:
        return "No campaign data found."

    campaign = report["campaign"]
    counts = report["planned_topic_status_counts"]
    metrics = report["publication_metrics"]
    lines = [
        f"# Campaign Retrospective: {campaign['name']}",
        "",
        f"- Campaign ID: {campaign['id']}",
        f"- Status: {campaign.get('status') or 'n/a'}",
        f"- Window: {campaign.get('start_date') or 'n/a'} to {campaign.get('end_date') or 'n/a'}",
    ]
    if campaign.get("goal"):
        lines.append(f"- Goal: {campaign['goal']}")

    lines.extend([
        "",
        "## Planned Topics",
        "",
        f"- Total: {counts.get('total', 0)}",
        f"- Generated: {counts.get('generated', 0)}",
        f"- Planned: {counts.get('planned', 0)}",
        f"- Skipped: {counts.get('skipped', 0)}",
        "",
        "## Publications",
        "",
        f"- Published items: {metrics['published_items']}",
        f"- Total engagement score: {metrics['total_engagement_score']:.2f}",
        f"- Average engagement score: {metrics['avg_engagement_score']:.2f}",
    ])

    if metrics["platform_counts"]:
        for platform, count in metrics["platform_counts"].items():
            stats = metrics["platforms"][platform]
            lines.append(
                f"- {platform}: {count} published, "
                f"{stats['total_engagement_score']:.2f} engagement"
            )
    else:
        lines.append("- No published campaign content yet.")

    lines.extend(["", "## Top Content", ""])
    if report["top_content"]:
        for item in report["top_content"]:
            platforms = ", ".join(item["published_platforms"]) or "unpublished"
            lines.extend([
                (
                    f"- Content #{item['content_id']} ({item['topic']}): "
                    f"{item['combined_engagement_score']:.2f} engagement on {platforms}"
                ),
                f"  - {_content_preview(item['content'])}",
            ])
    else:
        lines.append("- No published content to rank yet.")

    lines.extend(["", "## Missed Topics", ""])
    if report["missed_topics"]:
        for topic in report["missed_topics"]:
            target = topic.get("target_date") or "unscheduled"
            angle = f" - {topic['angle']}" if topic.get("angle") else ""
            lines.append(f"- {topic['topic']}{angle} ({target}, {topic['status']})")
    else:
        lines.append("- No missed planned topics.")

    lines.extend(["", "## Recommendations", ""])
    for recommendation in report["recommendations"]:
        lines.append(f"- {recommendation}")

    return "\n".join(lines)
