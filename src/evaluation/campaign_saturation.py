"""Campaign topic saturation reporting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from storage.db import Database


@dataclass
class CampaignTopicSaturationRow:
    """Per-topic saturation counts and action."""

    topic: str
    planned_count: int
    generated_count: int
    published_count: int
    last_published_at: str | None
    saturation_level: str
    recommendation: str


@dataclass
class CampaignSaturationReport:
    """Topic saturation report for one campaign."""

    campaign: dict
    period_days: int
    period_start: datetime
    period_end: datetime
    min_published: int
    topics: list[CampaignTopicSaturationRow]
    summary: dict[str, int]


class CampaignSaturationAnalyzer:
    """Analyze repeated topic coverage within campaign-linked content."""

    def __init__(self, db: Database):
        self.db = db

    def report(
        self,
        campaign_id: int | None = None,
        active: bool = False,
        days: int = 30,
        min_published: int = 3,
    ) -> CampaignSaturationReport | None:
        """Build a saturation report for an explicit, active, or recent campaign."""
        campaign = self._resolve_campaign(campaign_id=campaign_id, active=active)
        if campaign is None:
            return None

        period_end = datetime.now(timezone.utc)
        period_start = period_end - timedelta(days=max(days, 0))
        rows = self._topic_rows(campaign["id"], period_start)
        topics = [
            self._saturation_row(row, min_published=max(min_published, 1))
            for row in rows
        ]

        summary = {
            "topic_count": len(topics),
            "low": sum(1 for row in topics if row.saturation_level == "low"),
            "balanced": sum(1 for row in topics if row.saturation_level == "balanced"),
            "high": sum(1 for row in topics if row.saturation_level == "high"),
            "pause": sum(1 for row in topics if row.recommendation == "pause"),
            "diversify": sum(1 for row in topics if row.recommendation == "diversify"),
            "continue": sum(1 for row in topics if row.recommendation == "continue"),
        }

        return CampaignSaturationReport(
            campaign=campaign,
            period_days=days,
            period_start=period_start,
            period_end=period_end,
            min_published=max(min_published, 1),
            topics=topics,
            summary=summary,
        )

    def _resolve_campaign(
        self,
        campaign_id: int | None,
        active: bool,
    ) -> dict | None:
        if campaign_id is not None:
            return self.db.get_campaign(campaign_id)
        if active:
            return self.db.get_active_campaign()

        cursor = self.db.conn.execute(
            """SELECT *
               FROM content_campaigns
               WHERE status IN ('active', 'completed')
               ORDER BY
                   CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                   start_date DESC NULLS LAST,
                   created_at DESC,
                   id DESC
               LIMIT 1"""
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def _topic_rows(self, campaign_id: int, period_start: datetime) -> list[dict]:
        cursor = self.db.conn.execute(
            """SELECT pt.topic,
                      COUNT(DISTINCT pt.id) AS planned_count,
                      COUNT(DISTINCT CASE
                          WHEN gc.id IS NOT NULL
                           AND datetime(gc.created_at) >= datetime(?)
                          THEN gc.id
                      END) AS generated_count,
                      COUNT(DISTINCT CASE
                          WHEN cp.status = 'published'
                           AND cp.published_at IS NOT NULL
                           AND datetime(cp.published_at) >= datetime(?)
                          THEN gc.id
                      END) AS published_count,
                      MAX(CASE
                          WHEN cp.status = 'published'
                           AND cp.published_at IS NOT NULL
                           AND datetime(cp.published_at) >= datetime(?)
                          THEN cp.published_at
                      END) AS last_published_at
               FROM planned_topics pt
               LEFT JOIN generated_content gc ON gc.id = pt.content_id
               LEFT JOIN content_publications cp ON cp.content_id = gc.id
               WHERE pt.campaign_id = ?
                 AND pt.topic IS NOT NULL
               GROUP BY pt.topic
               ORDER BY lower(pt.topic) ASC""",
            (
                period_start.isoformat(),
                period_start.isoformat(),
                period_start.isoformat(),
                campaign_id,
            ),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _saturation_row(
        self,
        row: dict,
        min_published: int,
    ) -> CampaignTopicSaturationRow:
        planned_count = int(row["planned_count"] or 0)
        generated_count = int(row["generated_count"] or 0)
        published_count = int(row["published_count"] or 0)

        if published_count == 0:
            saturation_level = "low"
            recommendation = "continue"
        elif published_count >= min_published:
            saturation_level = "high"
            recommendation = "pause"
        elif generated_count >= min_published:
            saturation_level = "balanced"
            recommendation = "diversify"
        else:
            saturation_level = "balanced"
            recommendation = "continue"

        return CampaignTopicSaturationRow(
            topic=row["topic"],
            planned_count=planned_count,
            generated_count=generated_count,
            published_count=published_count,
            last_published_at=row["last_published_at"],
            saturation_level=saturation_level,
            recommendation=recommendation,
        )
