"""Campaign retrospective reporting and export helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from storage.db import Database

CONTENT_EXCERPT_LENGTH = 160


def truncate_content(content: str | None, max_length: int = CONTENT_EXCERPT_LENGTH) -> str:
    """Return a deterministic single-line content excerpt."""
    text = " ".join((content or "").split())
    if len(text) <= max_length:
        return text
    return f"{text[: max_length - 3]}..."


@dataclass
class CampaignRetrospective:
    """Structured campaign retrospective data."""

    campaign: dict[str, Any]
    planned_topic_status_counts: dict[str, int]
    totals: dict[str, int | float]
    generated_content: list[dict[str, Any]]
    published_outcomes: list[dict[str, Any]]
    platform_outcomes: dict[str, dict[str, int | float]]
    top_performing_content: list[dict[str, Any]]
    missed_topics: list[dict[str, Any]]
    recommended_follow_up_ideas: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign": self.campaign,
            "planned_topic_status_counts": self.planned_topic_status_counts,
            "totals": self.totals,
            "generated_content": self.generated_content,
            "published_outcomes": self.published_outcomes,
            "platform_outcomes": self.platform_outcomes,
            "top_performing_content": self.top_performing_content,
            "missed_topics": self.missed_topics,
            "recommended_follow_up_ideas": self.recommended_follow_up_ideas,
        }


def _round_score(value: Any) -> float | None:
    return round(float(value), 2) if value is not None else None


def _unique_topics(topic_rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    topics: dict[int, dict[str, Any]] = {}
    for row in topic_rows:
        topics.setdefault(row["planned_topic_id"], row)
    return topics


class CampaignRetrospectiveExporter:
    """Build campaign retrospectives from campaign planning and outcome tables."""

    def __init__(self, db: Database):
        self.db = db

    def build(
        self,
        campaign_id: int,
        *,
        include_content: bool = False,
        top_limit: int = 5,
    ) -> CampaignRetrospective | None:
        campaign = self.db.get_campaign(campaign_id)
        if campaign is None:
            return None

        topic_rows = self._topic_rows(campaign_id)
        generated_content = self._content_items(topic_rows, include_content)
        published_outcomes = [
            publication
            for item in generated_content
            for publication in item["publications"]
            if publication["status"] == "published"
        ]
        status_counts = self._status_counts(topic_rows)
        missed_topics = self._missed_topics(topic_rows)
        top_content = self._top_content(generated_content, top_limit)

        return CampaignRetrospective(
            campaign=campaign,
            planned_topic_status_counts=status_counts,
            totals={
                "planned_topics": status_counts["total"],
                "generated_topics": sum(
                    1
                    for row in _unique_topics(topic_rows).values()
                    if row["content_id"] is not None
                ),
                "published_items": len(published_outcomes),
                "missed_topics": len(missed_topics),
                "avg_engagement_score": self._average(
                    item["combined_engagement_score"]
                    for item in generated_content
                    if item["published_platforms"]
                ),
            },
            generated_content=generated_content,
            published_outcomes=published_outcomes,
            platform_outcomes=self._platform_outcomes(published_outcomes),
            top_performing_content=top_content,
            missed_topics=missed_topics,
            recommended_follow_up_ideas=self._follow_up_ideas(
                missed_topics,
                top_content,
                generated_content,
            ),
        )

    def _topic_rows(self, campaign_id: int) -> list[dict[str, Any]]:
        cursor = self.db.conn.execute(
            """SELECT pt.id AS planned_topic_id,
                      pt.topic,
                      pt.angle,
                      pt.source_material,
                      pt.target_date,
                      pt.status AS topic_status,
                      pt.created_at AS planned_at,
                      gc.id AS content_id,
                      gc.content_type,
                      gc.content,
                      gc.eval_score,
                      gc.published AS legacy_published,
                      gc.published_url AS legacy_published_url,
                      gc.published_at AS legacy_published_at,
                      gc.created_at AS content_created_at,
                      cp.id AS publication_id,
                      cp.platform,
                      cp.status AS publication_status,
                      cp.platform_post_id,
                      cp.platform_url,
                      cp.published_at AS publication_published_at,
                      CASE
                          WHEN cp.platform = 'x' THEN pe.engagement_score
                          WHEN cp.platform = 'bluesky' THEN be.engagement_score
                          ELSE NULL
                      END AS engagement_score
               FROM planned_topics pt
               LEFT JOIN generated_content gc ON gc.id = pt.content_id
               LEFT JOIN content_publications cp ON cp.content_id = gc.id
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM post_engagement
                   WHERE engagement_score IS NOT NULL
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM bluesky_engagement
                   WHERE engagement_score IS NOT NULL
               ) be ON be.content_id = gc.id AND be.rn = 1
               WHERE pt.campaign_id = ?
               ORDER BY pt.target_date ASC NULLS LAST,
                        pt.created_at ASC,
                        pt.id ASC,
                        cp.platform ASC""",
            (campaign_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    @staticmethod
    def _status_counts(topic_rows: list[dict[str, Any]]) -> dict[str, int]:
        counts = {"planned": 0, "generated": 0, "skipped": 0}
        for row in _unique_topics(topic_rows).values():
            status = row["topic_status"] or "planned"
            counts[status] = counts.get(status, 0) + 1
        counts["total"] = len(_unique_topics(topic_rows))
        return counts

    def _content_items(
        self,
        topic_rows: list[dict[str, Any]],
        include_content: bool,
    ) -> list[dict[str, Any]]:
        items: dict[int, dict[str, Any]] = {}
        for row in topic_rows:
            content_id = row["content_id"]
            if content_id is None:
                continue

            item = items.setdefault(
                content_id,
                {
                    "content_id": content_id,
                    "planned_topic_id": row["planned_topic_id"],
                    "topic": row["topic"],
                    "angle": row["angle"],
                    "content_type": row["content_type"],
                    "eval_score": row["eval_score"],
                    "created_at": row["content_created_at"],
                    "published_platforms": [],
                    "combined_engagement_score": 0.0,
                    "publications": [],
                },
            )
            if include_content:
                item["content_excerpt"] = truncate_content(row["content"])

            if row["publication_id"] is None:
                continue

            publication = {
                "content_id": content_id,
                "planned_topic_id": row["planned_topic_id"],
                "topic": row["topic"],
                "platform": row["platform"],
                "status": row["publication_status"],
                "platform_post_id": row["platform_post_id"],
                "platform_url": row["platform_url"],
                "published_at": row["publication_published_at"],
                "engagement_score": _round_score(row["engagement_score"]),
            }
            item["publications"].append(publication)
            if row["publication_status"] == "published" and row["platform"]:
                item["published_platforms"].append(row["platform"])
                if row["engagement_score"] is not None:
                    item["combined_engagement_score"] += float(row["engagement_score"])

        for item in items.values():
            item["published_platforms"] = sorted(set(item["published_platforms"]))
            item["publications"].sort(
                key=lambda publication: (
                    publication["published_at"] or "",
                    publication["platform"] or "",
                )
            )
            item["combined_engagement_score"] = round(
                item["combined_engagement_score"],
                2,
            )

        return sorted(
            items.values(),
            key=lambda item: (item["created_at"] or "", item["content_id"]),
        )

    @staticmethod
    def _missed_topics(topic_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        missed = []
        for row in _unique_topics(topic_rows).values():
            if row["content_id"] is None and row["topic_status"] != "skipped":
                missed.append(
                    {
                        "planned_topic_id": row["planned_topic_id"],
                        "topic": row["topic"],
                        "angle": row["angle"],
                        "target_date": row["target_date"],
                        "status": row["topic_status"] or "planned",
                    }
                )
        return missed

    @staticmethod
    def _top_content(
        generated_content: list[dict[str, Any]],
        top_limit: int,
    ) -> list[dict[str, Any]]:
        published = [item for item in generated_content if item["published_platforms"]]
        return sorted(
            published,
            key=lambda item: (
                item["combined_engagement_score"],
                item["eval_score"] or 0.0,
                item["content_id"],
            ),
            reverse=True,
        )[:top_limit]

    @staticmethod
    def _platform_outcomes(
        published_outcomes: list[dict[str, Any]],
    ) -> dict[str, dict[str, int | float]]:
        platforms: dict[str, dict[str, int | float]] = {}
        for publication in published_outcomes:
            platform = publication["platform"] or "unknown"
            stats = platforms.setdefault(
                platform,
                {
                    "published_items": 0,
                    "engagement_count": 0,
                    "total_engagement_score": 0.0,
                    "avg_engagement_score": 0.0,
                },
            )
            stats["published_items"] += 1
            score = publication["engagement_score"]
            if score is not None:
                stats["engagement_count"] += 1
                stats["total_engagement_score"] += float(score)

        for stats in platforms.values():
            total = float(stats["total_engagement_score"])
            count = int(stats["engagement_count"])
            stats["total_engagement_score"] = round(total, 2)
            stats["avg_engagement_score"] = round(total / count, 2) if count else 0.0
        return dict(sorted(platforms.items()))

    @staticmethod
    def _follow_up_ideas(
        missed_topics: list[dict[str, Any]],
        top_content: list[dict[str, Any]],
        generated_content: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        ideas = []
        for topic in missed_topics:
            ideas.append(
                {
                    "type": "cover_missed_topic",
                    "topic": topic["topic"],
                    "angle": topic["angle"],
                    "reason": "Planned topic did not produce generated content.",
                }
            )

        for item in top_content[:3]:
            ideas.append(
                {
                    "type": "repurpose_top_performer",
                    "topic": item["topic"],
                    "angle": item["angle"],
                    "source_content_id": item["content_id"],
                    "reason": "Published content had the strongest campaign engagement.",
                }
            )

        for item in generated_content:
            if item["published_platforms"]:
                continue
            ideas.append(
                {
                    "type": "publish_generated_content",
                    "topic": item["topic"],
                    "angle": item["angle"],
                    "source_content_id": item["content_id"],
                    "reason": "Content was generated but has no published outcome.",
                }
            )

        if not ideas:
            ideas.append(
                {
                    "type": "review_next_campaign",
                    "topic": None,
                    "angle": None,
                    "reason": "No missed or unpublished topics were found; review the strongest outcomes for the next campaign plan.",
                }
            )
        return ideas

    @staticmethod
    def _average(scores: Any) -> float:
        values = [float(score) for score in scores]
        return round(sum(values) / len(values), 2) if values else 0.0


def retrospective_to_dict(report: CampaignRetrospective) -> dict[str, Any]:
    return report.to_dict()


def format_markdown_retrospective(report: CampaignRetrospective) -> str:
    data = report.to_dict()
    campaign = data["campaign"]
    lines = [
        f"# Campaign Retrospective: {campaign['name']}",
        "",
        "## Campaign Metadata",
        f"- Campaign ID: {campaign['id']}",
        f"- Status: {campaign.get('status') or 'n/a'}",
        f"- Window: {campaign.get('start_date') or 'n/a'} to {campaign.get('end_date') or 'n/a'}",
    ]
    if campaign.get("goal"):
        lines.append(f"- Goal: {campaign['goal']}")
    lines.append("")

    counts = data["planned_topic_status_counts"]
    lines.extend(
        [
            "## Planned Topic Status Counts",
            f"- Total: {counts.get('total', 0)}",
            f"- Planned: {counts.get('planned', 0)}",
            f"- Generated: {counts.get('generated', 0)}",
            f"- Skipped: {counts.get('skipped', 0)}",
            "",
            "## Published Outcomes",
        ]
    )
    if data["published_outcomes"]:
        for publication in data["published_outcomes"]:
            score = publication["engagement_score"]
            score_text = f", engagement {score:.2f}" if score is not None else ""
            lines.append(
                f"- #{publication['content_id']} {publication['topic']} on "
                f"{publication['platform']} ({publication['published_at'] or 'n/a'}{score_text})"
            )
    else:
        lines.append("- No published content for this campaign.")
    lines.append("")

    lines.append("## Platform Outcomes")
    if data["platform_outcomes"]:
        for platform, stats in data["platform_outcomes"].items():
            lines.append(
                f"- {platform}: {stats['published_items']} published, "
                f"{stats['engagement_count']} with engagement, "
                f"avg {stats['avg_engagement_score']:.2f}, "
                f"total {stats['total_engagement_score']:.2f}"
            )
    else:
        lines.append("- No platform outcomes yet.")
    lines.append("")

    lines.append("## Top-Performing Content")
    if data["top_performing_content"]:
        for item in data["top_performing_content"]:
            lines.append(
                f"- #{item['content_id']} {item['topic']} "
                f"({', '.join(item['published_platforms'])}; "
                f"engagement {item['combined_engagement_score']:.2f})"
            )
            if item.get("content_excerpt"):
                lines.append(f"  - Excerpt: {item['content_excerpt']}")
    else:
        lines.append("- No published content with outcomes to rank.")
    lines.append("")

    lines.append("## Missed Topics")
    if data["missed_topics"]:
        for topic in data["missed_topics"]:
            target = topic["target_date"] or "n/a"
            angle = f" - {topic['angle']}" if topic.get("angle") else ""
            lines.append(f"- {topic['topic']}{angle} (target {target})")
    else:
        lines.append("- No missed planned topics.")
    lines.append("")

    lines.append("## Recommended Follow-Up Ideas")
    for idea in data["recommended_follow_up_ideas"]:
        label = idea["type"].replace("_", " ")
        topic = idea["topic"] or "campaign review"
        lines.append(f"- {label}: {topic} - {idea['reason']}")

    return "\n".join(lines) + "\n"


class CampaignRetrospectiveGenerator:
    """Compatibility wrapper for report-style campaign retrospectives."""

    def __init__(self, db: Database) -> None:
        self.exporter = CampaignRetrospectiveExporter(db)

    def build_report(
        self,
        campaign_id: int,
        top_limit: int = 5,
    ) -> dict[str, Any] | None:
        report = self.exporter.build(
            campaign_id,
            include_content=True,
            top_limit=top_limit,
        )
        if report is None:
            return None

        data = report.to_dict()
        platform_split = data["platform_outcomes"]
        publication_metrics = {
            "published_items": data["totals"]["published_items"],
            "platform_counts": {
                platform: stats["published_items"]
                for platform, stats in platform_split.items()
            },
            "platforms": platform_split,
            "total_engagement_score": round(
                sum(stats["total_engagement_score"] for stats in platform_split.values()),
                2,
            ),
            "avg_engagement_score": data["totals"]["avg_engagement_score"],
            "engagement_count": sum(
                int(stats["engagement_count"]) for stats in platform_split.values()
            ),
        }
        top_content = []
        for item in data["top_performing_content"]:
            top_content.append(
                {
                    "content_id": item["content_id"],
                    "topic": item["topic"],
                    "published_platforms": item["published_platforms"],
                    "combined_engagement_score": item["combined_engagement_score"],
                    "content": item.get("content_excerpt", ""),
                }
            )

        return {
            "campaign": data["campaign"],
            "planned_topic_status_counts": data["planned_topic_status_counts"],
            "publication_metrics": publication_metrics,
            "planned_topics": data["totals"]["planned_topics"],
            "generated_topics": data["totals"]["generated_topics"],
            "published_items": data["totals"]["published_items"],
            "avg_engagement_score": data["totals"]["avg_engagement_score"],
            "top_content": top_content,
            "missed_topics": data["missed_topics"],
            "platform_split": platform_split,
            "recommendations": _legacy_recommendations(data),
        }


def _legacy_recommendations(data: dict[str, Any]) -> list[str]:
    recommendations: list[str] = []
    if data["missed_topics"]:
        topic_names = ", ".join(topic["topic"] for topic in data["missed_topics"][:3])
        recommendations.append(
            f"Schedule or intentionally skip missed planned topics: {topic_names}."
        )
    unpublished = [
        item for item in data["generated_content"] if not item["published_platforms"]
    ]
    if unpublished:
        recommendations.append(
            "Move generated campaign content into the publish queue before adding new topics."
        )
    if data["top_performing_content"]:
        best = data["top_performing_content"][0]
        recommendations.append(
            f"Continue the strongest angle, {best['topic']}, and repurpose content #{best['content_id']}."
        )
    if not recommendations:
        recommendations.append(
            "Close the campaign or create the next plan using the top-performing platform and topic mix."
        )
    return recommendations


def report_to_dict(report: dict[str, Any] | None) -> dict[str, Any]:
    if report is None:
        return {"error": "No campaign data found"}
    return report


def format_json_report(report: dict[str, Any] | None) -> str:
    return json.dumps(report_to_dict(report), indent=2, sort_keys=True)


def format_markdown_report(report: dict[str, Any] | None) -> str:
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

    lines.extend(
        [
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
        ]
    )

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
            lines.extend(
                [
                    (
                        f"- Content #{item['content_id']} ({item['topic']}): "
                        f"{item['combined_engagement_score']:.2f} engagement on {platforms}"
                    ),
                    f"  - {item.get('content') or ''}".rstrip(),
                ]
            )
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
