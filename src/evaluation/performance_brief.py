"""Weekly performance brief built from publication and engagement history."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class BriefPublication:
    """One platform publication for a generated content item."""

    publication_id: int | None
    platform: str
    status: str | None
    platform_post_id: str | None
    platform_url: str | None
    published_at: str | None
    engagement_score: float | None
    like_count: int
    share_count: int
    reply_count: int
    quote_count: int


@dataclass(frozen=True)
class BriefVariant:
    """Available generated-content variant metadata."""

    id: int
    platform: str
    variant_type: str
    created_at: str | None


@dataclass(frozen=True)
class BriefContentItem:
    """Published/generated content with campaign and engagement context."""

    content_id: int
    content_type: str
    content_format: str | None
    content: str
    eval_score: float | None
    auto_quality: str | None
    created_at: str | None
    generated_in_week: bool
    published_in_week: bool
    planned_topic_id: int | None
    planned_topic: str | None
    planned_angle: str | None
    planned_target_date: str | None
    campaign_id: int | None
    campaign_name: str | None
    campaign_goal: str | None
    publications: list[BriefPublication]
    variants: list[BriefVariant]
    combined_engagement_score: float


@dataclass(frozen=True)
class BriefPlannedTopic:
    """Planned campaign topic due in the brief window."""

    planned_topic_id: int
    topic: str
    angle: str | None
    target_date: str | None
    status: str
    content_id: int | None
    campaign_id: int | None
    campaign_name: str | None


@dataclass(frozen=True)
class PerformanceBrief:
    """Structured weekly performance brief."""

    week_start: str
    week_end: str
    generated_count: int
    published_count: int
    publication_count: int
    platform_summary: dict[str, dict[str, float | int]]
    published: list[BriefContentItem]
    resonated: list[BriefContentItem]
    underperformed: list[BriefContentItem]
    planned_topics: list[BriefPlannedTopic]
    try_next: list[str]


def parse_week_start(value: str | date | datetime | None) -> date:
    """Parse a week-start date, defaulting to the current UTC ISO week."""
    if value is None:
        today = datetime.now(timezone.utc).date()
        return today - timedelta(days=today.weekday())
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


class PerformanceBriefBuilder:
    """Build weekly operator briefs from the SQLite storage layer."""

    def __init__(self, db) -> None:
        self.db = db

    def build(self, week_start: str | date | datetime | None = None) -> PerformanceBrief:
        start_date = parse_week_start(week_start)
        start_dt = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
        end_dt = start_dt + timedelta(days=7)
        start = start_dt.isoformat()
        end = end_dt.isoformat()

        content_rows = self._content_rows(start, end)
        content_ids = [row["content_id"] for row in content_rows]
        publications = self._publications_by_content(content_ids)
        variants = self._variants_by_content(content_ids)

        items = [
            self._content_item_from_row(
                row,
                publications.get(row["content_id"], []),
                variants.get(row["content_id"], []),
            )
            for row in content_rows
        ]
        published = [item for item in items if item.published_in_week]
        resonated, underperformed = self._classify_items(published)
        planned_topics = self._planned_topics(start_date.isoformat(), (start_date + timedelta(days=6)).isoformat())

        return PerformanceBrief(
            week_start=start_date.isoformat(),
            week_end=(start_date + timedelta(days=7)).isoformat(),
            generated_count=sum(1 for item in items if item.generated_in_week),
            published_count=len(published),
            publication_count=sum(len(item.publications) for item in published),
            platform_summary=self._platform_summary(published),
            published=published,
            resonated=resonated,
            underperformed=underperformed,
            planned_topics=planned_topics,
            try_next=self._try_next(published, resonated, underperformed, planned_topics),
        )

    def _content_rows(self, start: str, end: str) -> list[dict]:
        cursor = self.db.conn.execute(
            """WITH latest_x AS (
                   SELECT content_id, tweet_id, like_count, retweet_count,
                          reply_count, quote_count, engagement_score
                   FROM (
                       SELECT *,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                              ) AS rn
                       FROM post_engagement
                   )
                   WHERE rn = 1
               ),
               latest_bluesky AS (
                   SELECT content_id, bluesky_uri, like_count, repost_count,
                          reply_count, quote_count, engagement_score
                   FROM (
                       SELECT *,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                              ) AS rn
                       FROM bluesky_engagement
                   )
                   WHERE rn = 1
               ),
               content_window AS (
                   SELECT DISTINCT gc.id
                   FROM generated_content gc
                   LEFT JOIN content_publications cp
                          ON cp.content_id = gc.id
                         AND cp.status = 'published'
                   WHERE (gc.created_at >= ? AND gc.created_at < ?)
                      OR (gc.published_at >= ? AND gc.published_at < ?)
                      OR (cp.published_at >= ? AND cp.published_at < ?)
               )
               SELECT gc.id AS content_id,
                      gc.content_type,
                      gc.content_format,
                      gc.content,
                      gc.eval_score,
                      gc.auto_quality,
                      gc.created_at,
                      CASE WHEN gc.created_at >= ? AND gc.created_at < ? THEN 1 ELSE 0 END
                          AS generated_in_week,
                      CASE
                          WHEN gc.published_at >= ? AND gc.published_at < ? THEN 1
                          WHEN EXISTS (
                              SELECT 1 FROM content_publications cpw
                              WHERE cpw.content_id = gc.id
                                AND cpw.status = 'published'
                                AND cpw.published_at >= ?
                                AND cpw.published_at < ?
                          ) THEN 1
                          ELSE 0
                      END AS published_in_week,
                      pt.id AS planned_topic_id,
                      pt.topic AS planned_topic,
                      pt.angle AS planned_angle,
                      pt.target_date AS planned_target_date,
                      cc.id AS campaign_id,
                      cc.name AS campaign_name,
                      cc.goal AS campaign_goal,
                      lx.engagement_score AS x_engagement_score,
                      lb.engagement_score AS bluesky_engagement_score
               FROM content_window cw
               INNER JOIN generated_content gc ON gc.id = cw.id
               LEFT JOIN planned_topics pt ON pt.content_id = gc.id
               LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
               LEFT JOIN latest_x lx ON lx.content_id = gc.id
               LEFT JOIN latest_bluesky lb ON lb.content_id = gc.id
               ORDER BY published_in_week DESC,
                        COALESCE(gc.published_at, gc.created_at) DESC,
                        gc.id DESC""",
            (start, end, start, end, start, end, start, end, start, end, start, end),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _publications_by_content(self, content_ids: list[int]) -> dict[int, list[BriefPublication]]:
        if not content_ids:
            return {}
        placeholders = ", ".join("?" for _ in content_ids)
        cursor = self.db.conn.execute(
            f"""WITH latest_x AS (
                   SELECT content_id, tweet_id, like_count, retweet_count,
                          reply_count, quote_count, engagement_score
                   FROM (
                       SELECT *,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                              ) AS rn
                       FROM post_engagement
                   )
                   WHERE rn = 1
               ),
               latest_bluesky AS (
                   SELECT content_id, bluesky_uri, like_count, repost_count,
                          reply_count, quote_count, engagement_score
                   FROM (
                       SELECT *,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                              ) AS rn
                       FROM bluesky_engagement
                   )
                   WHERE rn = 1
               ),
               publication_rows AS (
                   SELECT cp.content_id,
                          cp.id AS publication_id,
                          cp.platform,
                          cp.status,
                          cp.platform_post_id,
                          cp.platform_url,
                          cp.published_at,
                          CASE
                              WHEN cp.platform = 'x' THEN lx.engagement_score
                              WHEN cp.platform = 'bluesky' THEN lb.engagement_score
                          END AS engagement_score,
                          CASE
                              WHEN cp.platform = 'x' THEN lx.like_count
                              WHEN cp.platform = 'bluesky' THEN lb.like_count
                              ELSE 0
                          END AS like_count,
                          CASE
                              WHEN cp.platform = 'x' THEN lx.retweet_count
                              WHEN cp.platform = 'bluesky' THEN lb.repost_count
                              ELSE 0
                          END AS share_count,
                          CASE
                              WHEN cp.platform = 'x' THEN lx.reply_count
                              WHEN cp.platform = 'bluesky' THEN lb.reply_count
                              ELSE 0
                          END AS reply_count,
                          CASE
                              WHEN cp.platform = 'x' THEN lx.quote_count
                              WHEN cp.platform = 'bluesky' THEN lb.quote_count
                              ELSE 0
                          END AS quote_count
                   FROM content_publications cp
                   LEFT JOIN latest_x lx ON lx.content_id = cp.content_id
                   LEFT JOIN latest_bluesky lb ON lb.content_id = cp.content_id
                   WHERE cp.content_id IN ({placeholders})
                     AND cp.status = 'published'
                   UNION ALL
                   SELECT gc.id AS content_id,
                          NULL AS publication_id,
                          'x' AS platform,
                          'published' AS status,
                          gc.tweet_id AS platform_post_id,
                          gc.published_url AS platform_url,
                          gc.published_at,
                          lx.engagement_score,
                          lx.like_count,
                          lx.retweet_count AS share_count,
                          lx.reply_count,
                          lx.quote_count
                   FROM generated_content gc
                   LEFT JOIN latest_x lx ON lx.content_id = gc.id
                   WHERE gc.id IN ({placeholders})
                     AND gc.published = 1
                     AND NOT EXISTS (
                         SELECT 1 FROM content_publications cp
                         WHERE cp.content_id = gc.id
                           AND cp.platform = 'x'
                           AND cp.status = 'published'
                     )
               )
               SELECT *
               FROM publication_rows
               ORDER BY content_id, published_at ASC NULLS LAST, platform ASC""",
            (*content_ids, *content_ids),
        )
        grouped: dict[int, list[BriefPublication]] = {}
        for row in cursor.fetchall():
            grouped.setdefault(row["content_id"], []).append(
                BriefPublication(
                    publication_id=row["publication_id"],
                    platform=row["platform"],
                    status=row["status"],
                    platform_post_id=row["platform_post_id"],
                    platform_url=row["platform_url"],
                    published_at=row["published_at"],
                    engagement_score=(
                        round(float(row["engagement_score"]), 2)
                        if row["engagement_score"] is not None
                        else None
                    ),
                    like_count=int(row["like_count"] or 0),
                    share_count=int(row["share_count"] or 0),
                    reply_count=int(row["reply_count"] or 0),
                    quote_count=int(row["quote_count"] or 0),
                )
            )
        return grouped

    def _variants_by_content(self, content_ids: list[int]) -> dict[int, list[BriefVariant]]:
        if not content_ids:
            return {}
        placeholders = ", ".join("?" for _ in content_ids)
        cursor = self.db.conn.execute(
            f"""SELECT id, content_id, platform, variant_type, created_at
                FROM content_variants
                WHERE content_id IN ({placeholders})
                ORDER BY content_id, platform, variant_type, id""",
            content_ids,
        )
        grouped: dict[int, list[BriefVariant]] = {}
        for row in cursor.fetchall():
            grouped.setdefault(row["content_id"], []).append(
                BriefVariant(
                    id=row["id"],
                    platform=row["platform"],
                    variant_type=row["variant_type"],
                    created_at=row["created_at"],
                )
            )
        return grouped

    def _planned_topics(self, start_date: str, end_date: str) -> list[BriefPlannedTopic]:
        cursor = self.db.conn.execute(
            """SELECT pt.id AS planned_topic_id,
                      pt.topic,
                      pt.angle,
                      pt.target_date,
                      pt.status,
                      pt.content_id,
                      cc.id AS campaign_id,
                      cc.name AS campaign_name
               FROM planned_topics pt
               LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
               WHERE pt.target_date >= ?
                 AND pt.target_date <= ?
               ORDER BY pt.target_date ASC, pt.created_at ASC, pt.id ASC""",
            (start_date, end_date),
        )
        return [
            BriefPlannedTopic(
                planned_topic_id=row["planned_topic_id"],
                topic=row["topic"],
                angle=row["angle"],
                target_date=row["target_date"],
                status=row["status"],
                content_id=row["content_id"],
                campaign_id=row["campaign_id"],
                campaign_name=row["campaign_name"],
            )
            for row in cursor.fetchall()
        ]

    def _content_item_from_row(
        self,
        row: dict,
        publications: list[BriefPublication],
        variants: list[BriefVariant],
    ) -> BriefContentItem:
        combined = sum(
            publication.engagement_score or 0.0
            for publication in publications
        )
        return BriefContentItem(
            content_id=row["content_id"],
            content_type=row["content_type"],
            content_format=row["content_format"],
            content=row["content"],
            eval_score=row["eval_score"],
            auto_quality=row["auto_quality"],
            created_at=row["created_at"],
            generated_in_week=bool(row["generated_in_week"]),
            published_in_week=bool(row["published_in_week"]),
            planned_topic_id=row["planned_topic_id"],
            planned_topic=row["planned_topic"],
            planned_angle=row["planned_angle"],
            planned_target_date=row["planned_target_date"],
            campaign_id=row["campaign_id"],
            campaign_name=row["campaign_name"],
            campaign_goal=row["campaign_goal"],
            publications=publications,
            variants=variants,
            combined_engagement_score=round(combined, 2),
        )

    def _classify_items(
        self,
        published: list[BriefContentItem],
    ) -> tuple[list[BriefContentItem], list[BriefContentItem]]:
        scored = [
            item for item in published
            if item.combined_engagement_score > 0 or item.auto_quality
        ]
        if not scored:
            return [], []

        scores = [item.combined_engagement_score for item in scored]
        avg_score = sum(scores) / len(scores)
        resonated = [
            item for item in scored
            if item.auto_quality == "resonated"
            or item.combined_engagement_score >= avg_score
        ]
        underperformed = [
            item for item in scored
            if item.auto_quality == "low_resonance"
            or item.combined_engagement_score < avg_score
        ]
        resonated.sort(key=lambda item: item.combined_engagement_score, reverse=True)
        underperformed.sort(key=lambda item: item.combined_engagement_score)
        return resonated[:5], underperformed[:5]

    def _platform_summary(self, published: list[BriefContentItem]) -> dict[str, dict[str, float | int]]:
        summary: dict[str, dict[str, float | int]] = {}
        for item in published:
            for publication in item.publications:
                stats = summary.setdefault(
                    publication.platform,
                    {
                        "publication_count": 0,
                        "engagement_count": 0,
                        "total_engagement_score": 0.0,
                        "avg_engagement_score": 0.0,
                    },
                )
                stats["publication_count"] += 1
                if publication.engagement_score is not None:
                    stats["engagement_count"] += 1
                    stats["total_engagement_score"] += publication.engagement_score
        for stats in summary.values():
            total = float(stats["total_engagement_score"])
            count = int(stats["engagement_count"])
            stats["total_engagement_score"] = round(total, 2)
            stats["avg_engagement_score"] = round(total / count, 2) if count else 0.0
        return summary

    def _try_next(
        self,
        published: list[BriefContentItem],
        resonated: list[BriefContentItem],
        underperformed: list[BriefContentItem],
        planned_topics: list[BriefPlannedTopic],
    ) -> list[str]:
        suggestions: list[str] = []

        resonant_formats = [
            item.content_format for item in resonated if item.content_format
        ]
        if resonant_formats:
            suggestions.append(
                "Repeat the strongest format signal: "
                f"{_most_common(resonant_formats).replace('_', ' ')}."
            )

        weak_formats = [
            item.content_format for item in underperformed if item.content_format
        ]
        if weak_formats:
            suggestions.append(
                "Retest or reframe weak formats instead of repeating them unchanged: "
                f"{_most_common(weak_formats).replace('_', ' ')}."
            )

        unfilled = [
            topic for topic in planned_topics
            if topic.status == "planned" and topic.content_id is None
        ]
        if unfilled:
            topic = unfilled[0]
            angle = f" ({topic.angle})" if topic.angle else ""
            campaign = f" for {topic.campaign_name}" if topic.campaign_name else ""
            suggestions.append(f"Fill planned topic #{topic.planned_topic_id}: {topic.topic}{angle}{campaign}.")

        if published and not any(item.variants for item in published):
            suggestions.append("Create at least one platform-specific variant for high-scoring posts.")

        if not suggestions:
            suggestions.append("Keep the current cadence and collect another week of engagement data.")
        return suggestions[:5]


def brief_to_dict(brief: PerformanceBrief) -> dict[str, Any]:
    """Convert a brief dataclass to plain JSON-serializable data."""
    return asdict(brief)


def format_markdown_brief(brief: PerformanceBrief) -> str:
    """Format a weekly brief as operator-friendly Markdown."""
    lines = [
        f"# Weekly Performance Brief: {brief.week_start} to {brief.week_end}",
        "",
        "## Summary",
        "",
        f"- Generated: {brief.generated_count}",
        f"- Published content items: {brief.published_count}",
        f"- Platform publications: {brief.publication_count}",
    ]

    if brief.platform_summary:
        lines.extend(["", "## Platform Performance", ""])
        for platform, stats in sorted(brief.platform_summary.items()):
            lines.append(
                f"- {platform}: {stats['publication_count']} publications, "
                f"avg engagement {stats['avg_engagement_score']:.2f}, "
                f"total {stats['total_engagement_score']:.2f}"
            )

    lines.extend(["", "## Published", ""])
    if brief.published:
        for item in brief.published:
            lines.extend(_format_content_markdown(item))
    else:
        lines.append("- No published content found for this week.")

    lines.extend(["", "## Resonated", ""])
    if brief.resonated:
        for item in brief.resonated:
            lines.append(_format_item_headline(item))
    else:
        lines.append("- No resonant items identified yet.")

    lines.extend(["", "## Underperformed", ""])
    if brief.underperformed:
        for item in brief.underperformed:
            lines.append(_format_item_headline(item))
    else:
        lines.append("- No underperforming items identified yet.")

    lines.extend(["", "## Planned Topics", ""])
    if brief.planned_topics:
        for topic in brief.planned_topics:
            campaign = f", campaign #{topic.campaign_id} {topic.campaign_name}" if topic.campaign_id else ""
            content = f", content #{topic.content_id}" if topic.content_id else ""
            angle = f" - {topic.angle}" if topic.angle else ""
            lines.append(
                f"- Topic #{topic.planned_topic_id}: {topic.topic}{angle} "
                f"({topic.status}, target {topic.target_date or 'n/a'}{campaign}{content})"
            )
    else:
        lines.append("- No planned topics targeted this week.")

    lines.extend(["", "## Try Next", ""])
    for suggestion in brief.try_next:
        lines.append(f"- {suggestion}")

    return "\n".join(lines).rstrip() + "\n"


def _format_content_markdown(item: BriefContentItem) -> list[str]:
    topic_parts = []
    if item.planned_topic_id:
        topic = item.planned_topic or "planned topic"
        topic_parts.append(f"topic #{item.planned_topic_id} {topic}")
    if item.campaign_id:
        topic_parts.append(f"campaign #{item.campaign_id} {item.campaign_name}")
    metadata = f" ({'; '.join(topic_parts)})" if topic_parts else ""
    lines = [
        f"- {_format_item_headline(item)}{metadata}",
        f"  - Preview: {_preview(item.content)}",
    ]
    if item.publications:
        pub_bits = []
        for publication in item.publications:
            target = publication.platform_url or publication.platform_post_id or "no link/id"
            if publication.platform_url:
                target = f"[{publication.platform_post_id or publication.platform_url}]({publication.platform_url})"
            score = (
                f"{publication.engagement_score:.2f}"
                if publication.engagement_score is not None
                else "n/a"
            )
            pub_id = f"pub #{publication.publication_id}, " if publication.publication_id else ""
            pub_bits.append(f"{publication.platform} {pub_id}{target}, score {score}")
        lines.append(f"  - Publications: {'; '.join(pub_bits)}")
    if item.variants:
        variants = ", ".join(
            f"variant #{variant.id} {variant.platform}/{variant.variant_type}"
            for variant in item.variants
        )
        lines.append(f"  - Variants: {variants}")
    return lines


def _format_item_headline(item: BriefContentItem) -> str:
    quality = f", {item.auto_quality}" if item.auto_quality else ""
    content_format = f", {item.content_format}" if item.content_format else ""
    return (
        f"content #{item.content_id} "
        f"({item.content_type}{content_format}, engagement "
        f"{item.combined_engagement_score:.2f}{quality})"
    )


def _preview(content: str, limit: int = 150) -> str:
    text = " ".join((content or "").split())
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _most_common(values: list[str]) -> str:
    counts = {value: values.count(value) for value in set(values)}
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
