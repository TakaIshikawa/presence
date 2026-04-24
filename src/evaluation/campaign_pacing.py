"""Forward-looking pacing reports for active content campaigns."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from storage.db import Database


@dataclass
class CampaignPacingReport:
    """Campaign pacing status and recommended next actions."""

    campaign: dict
    status: str
    expected_progress: float | None
    actual_progress: float
    remaining_topics: list[dict]
    scheduled_items: list[dict]
    recommendations: list[dict]
    planned_topics: int
    published_items: int
    generated_unscheduled: int


class CampaignPacingAnalyzer:
    """Analyze whether a campaign is ahead, on track, or behind."""

    def __init__(self, db: Database, now: datetime | None = None):
        self.db = db
        self.now = _coerce_datetime(now) if now else datetime.now(timezone.utc)

    def report(
        self,
        campaign_id: int | None = None,
        active: bool = True,
    ) -> CampaignPacingReport | None:
        """Build a pacing report for an explicit or active campaign."""
        campaign = self._resolve_campaign(campaign_id=campaign_id, active=active)
        if campaign is None:
            return None

        rows = self._topic_rows(campaign["id"])
        planned_rows = [row for row in rows if row["topic_status"] != "skipped"]
        scheduled_items = self._scheduled_items(campaign["id"])
        scheduled_topic_ids = {
            item["planned_topic_id"]
            for item in scheduled_items
            if item.get("planned_topic_id") is not None
        }
        published_topic_ids = {
            row["planned_topic_id"]
            for row in planned_rows
            if row["planned_topic_id"] is not None and row["is_published"]
        }
        generated_unscheduled_ids = {
            row["planned_topic_id"]
            for row in planned_rows
            if row["content_id"] is not None
            and not row["is_published"]
            and row["planned_topic_id"] not in scheduled_topic_ids
        }

        total_topics = len(planned_rows)
        complete_or_committed = published_topic_ids | scheduled_topic_ids
        actual_progress = (
            round(min(len(complete_or_committed) / total_topics, 1.0), 3)
            if total_topics
            else 0.0
        )
        expected_progress = self._expected_progress(campaign)
        status = self._status(
            expected_progress=expected_progress,
            actual_progress=actual_progress,
            total_topics=total_topics,
        )
        remaining_topics = [
            self._remaining_topic(row, scheduled=row["planned_topic_id"] in scheduled_topic_ids)
            for row in planned_rows
            if row["planned_topic_id"] not in complete_or_committed
            and not row["is_published"]
        ]

        return CampaignPacingReport(
            campaign=campaign,
            status=status,
            expected_progress=expected_progress,
            actual_progress=actual_progress,
            remaining_topics=remaining_topics,
            scheduled_items=scheduled_items,
            recommendations=self._recommendations(
                status=status,
                total_topics=total_topics,
                remaining_topics=remaining_topics,
                generated_unscheduled=len(generated_unscheduled_ids),
                scheduled_items=len(scheduled_items),
            ),
            planned_topics=total_topics,
            published_items=len(published_topic_ids),
            generated_unscheduled=len(generated_unscheduled_ids),
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
               ORDER BY start_date DESC NULLS LAST, created_at DESC, id DESC
               LIMIT 1"""
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def _topic_rows(self, campaign_id: int) -> list[dict]:
        cursor = self.db.conn.execute(
            """SELECT pt.id AS planned_topic_id,
                      pt.topic,
                      pt.angle,
                      pt.target_date,
                      pt.status AS topic_status,
                      pt.content_id,
                      gc.content_type,
                      gc.content,
                      gc.published AS generated_published,
                      gc.published_at AS generated_published_at,
                      CASE
                        WHEN COALESCE(gc.published, 0) = 1 THEN 1
                        WHEN EXISTS (
                            SELECT 1
                            FROM content_publications cp
                            WHERE cp.content_id = pt.content_id
                              AND cp.status = 'published'
                        ) THEN 1
                        ELSE 0
                      END AS is_published
               FROM planned_topics pt
               LEFT JOIN generated_content gc ON gc.id = pt.content_id
               WHERE pt.campaign_id = ?
               ORDER BY pt.target_date ASC NULLS LAST, pt.created_at ASC, pt.id ASC""",
            (campaign_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _scheduled_items(self, campaign_id: int) -> list[dict]:
        cursor = self.db.conn.execute(
            """SELECT pq.id AS queue_id,
                      pq.content_id,
                      pq.scheduled_at,
                      pq.platform,
                      pq.status,
                      pt.id AS planned_topic_id,
                      pt.topic,
                      pt.angle,
                      gc.content_type
               FROM publish_queue pq
               INNER JOIN generated_content gc ON gc.id = pq.content_id
               INNER JOIN planned_topics pt ON pt.content_id = gc.id
               WHERE pt.campaign_id = ?
                 AND pq.status = 'queued'
               ORDER BY pq.scheduled_at ASC, pq.id ASC""",
            (campaign_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _expected_progress(self, campaign: dict) -> float | None:
        start = _parse_date(campaign.get("start_date"))
        end = _parse_date(campaign.get("end_date"))
        if start is None or end is None:
            return None
        if end < start:
            return None

        today = self.now.date()
        if today < start:
            return 0.0
        if today > end:
            return 1.0

        total_days = max((end - start).days + 1, 1)
        elapsed_days = min(max((today - start).days + 1, 0), total_days)
        return round(elapsed_days / total_days, 3)

    def _status(
        self,
        expected_progress: float | None,
        actual_progress: float,
        total_topics: int,
    ) -> str:
        if total_topics == 0:
            return "on_track"
        if expected_progress is None:
            return "ahead" if actual_progress >= 1.0 else "on_track"
        if actual_progress + 0.05 < expected_progress:
            return "behind"
        if actual_progress > expected_progress + 0.15:
            return "ahead"
        return "on_track"

    def _remaining_topic(self, row: dict, scheduled: bool) -> dict:
        return {
            "planned_topic_id": row["planned_topic_id"],
            "topic": row["topic"],
            "angle": row["angle"],
            "target_date": row["target_date"],
            "status": row["topic_status"],
            "content_id": row["content_id"],
            "generated": row["content_id"] is not None,
            "scheduled": scheduled,
        }

    def _recommendations(
        self,
        status: str,
        total_topics: int,
        remaining_topics: list[dict],
        generated_unscheduled: int,
        scheduled_items: int,
    ) -> list[dict]:
        if total_topics == 0:
            return [{
                "action": "no_action",
                "reason": "Campaign has no planned topics to pace.",
            }]
        if status == "behind" and generated_unscheduled:
            return [{
                "action": "schedule_existing",
                "reason": f"Schedule {generated_unscheduled} generated campaign item(s) before creating more.",
            }]
        if status == "behind" and remaining_topics:
            return [{
                "action": "generate_more",
                "reason": f"Generate content for {len(remaining_topics)} remaining planned topic(s).",
            }]
        if status == "ahead" and scheduled_items:
            return [{
                "action": "pause_campaign",
                "reason": "Campaign is ahead of the expected date pace with scheduled items already queued.",
            }]
        return [{
            "action": "no_action",
            "reason": "Campaign pacing is within the expected range.",
        }]


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        try:
            return date.fromisoformat(normalized[:10])
        except ValueError:
            return None
