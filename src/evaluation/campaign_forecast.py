"""Forecast active campaign pacing against planned topics."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class CampaignRecommendation:
    """Next campaign generation priority."""

    planned_topic_id: int | None
    topic: str | None
    angle: str | None
    target_date: str | None
    content_type: str
    reason: str


@dataclass(frozen=True)
class CampaignForecast:
    """Pacing forecast for one campaign."""

    campaign: dict[str, Any]
    planned_count: int
    generated_count: int
    queued_count: int
    overdue_count: int
    remaining_count: int
    days_remaining: int | None
    estimated_generation_rate: float
    required_generation_rate: float
    miss_risk: str
    overdue_topics: list[dict[str, Any]]
    remaining_topics: list[dict[str, Any]]
    recommendation: CampaignRecommendation


@dataclass(frozen=True)
class CampaignForecastReport:
    """Forecast report for active campaigns."""

    generated_at: str
    days: int
    campaign_id: int | None
    campaigns: list[CampaignForecast]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.combine(
                datetime.fromisoformat(text[:10]).date(),
                time.min,
                tzinfo=timezone.utc,
            )
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _days_until(value: str | None, now: datetime) -> int | None:
    target = _parse_datetime(value)
    if target is None:
        return None
    return (target.date() - now.date()).days


class CampaignForecaster:
    """Build campaign forecasts from planned topics and generation cadence."""

    def __init__(self, db):
        self.db = db

    def forecast(
        self,
        *,
        campaign_id: int | None = None,
        days: int = 14,
        now: datetime | None = None,
    ) -> CampaignForecastReport:
        if days <= 0:
            raise ValueError("days must be positive")
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)

        campaigns = self._resolve_campaigns(campaign_id)
        if campaign_id is not None and not campaigns:
            raise ValueError(f"Campaign {campaign_id} does not exist")

        forecasts = [
            self._forecast_campaign(campaign, days=days, now=now)
            for campaign in campaigns
        ]
        return CampaignForecastReport(
            generated_at=now.isoformat(),
            days=days,
            campaign_id=campaign_id,
            campaigns=forecasts,
        )

    def _resolve_campaigns(self, campaign_id: int | None) -> list[dict[str, Any]]:
        if campaign_id is not None:
            campaign = self.db.get_campaign(campaign_id)
            return [campaign] if campaign else []
        rows = self.db.conn.execute(
            """SELECT *
               FROM content_campaigns
               WHERE status = 'active'
               ORDER BY end_date ASC NULLS LAST, start_date ASC NULLS LAST, id ASC"""
        ).fetchall()
        return [dict(row) for row in rows]

    def _forecast_campaign(
        self,
        campaign: dict[str, Any],
        *,
        days: int,
        now: datetime,
    ) -> CampaignForecast:
        topics = self._topic_rows(campaign["id"])
        planned_topics = [topic for topic in topics if topic.get("status") != "skipped"]
        generated_topics = [
            topic for topic in planned_topics
            if topic.get("content_id") is not None or topic.get("status") == "generated"
        ]
        remaining_topics = [
            topic for topic in planned_topics
            if topic.get("content_id") is None and topic.get("status") == "planned"
        ]
        overdue_topics = [
            self._topic_summary(topic)
            for topic in remaining_topics
            if self._is_overdue(topic.get("target_date"), now)
        ]
        queued_count = self._queued_topic_count(campaign["id"])
        estimated_rate = self._estimated_generation_rate(campaign["id"], days, now)
        days_remaining = _days_until(campaign.get("end_date"), now)
        effective_days = self._effective_days_remaining(days_remaining)
        required_rate = (
            len(remaining_topics) / effective_days
            if remaining_topics and effective_days > 0
            else 0.0
        )

        return CampaignForecast(
            campaign=campaign,
            planned_count=len(planned_topics),
            generated_count=len(generated_topics),
            queued_count=queued_count,
            overdue_count=len(overdue_topics),
            remaining_count=len(remaining_topics),
            days_remaining=days_remaining,
            estimated_generation_rate=round(estimated_rate, 3),
            required_generation_rate=round(required_rate, 3),
            miss_risk=self._miss_risk(
                remaining_count=len(remaining_topics),
                overdue_count=len(overdue_topics),
                days_remaining=days_remaining,
                estimated_rate=estimated_rate,
                required_rate=required_rate,
            ),
            overdue_topics=overdue_topics,
            remaining_topics=[self._topic_summary(topic) for topic in remaining_topics],
            recommendation=self._recommendation(remaining_topics, campaign["id"], days, now),
        )

    def _topic_rows(self, campaign_id: int) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            """SELECT pt.*,
                      gc.content_type,
                      gc.created_at AS generated_at,
                      gc.published_at
               FROM planned_topics pt
               LEFT JOIN generated_content gc ON gc.id = pt.content_id
               WHERE pt.campaign_id = ?
               ORDER BY pt.target_date ASC NULLS LAST, pt.created_at ASC, pt.id ASC""",
            (campaign_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def _queued_topic_count(self, campaign_id: int) -> int:
        row = self.db.conn.execute(
            """SELECT COUNT(DISTINCT pt.id)
               FROM planned_topics pt
               INNER JOIN publish_queue pq ON pq.content_id = pt.content_id
               WHERE pt.campaign_id = ?
                 AND pq.status IN ('queued', 'held')""",
            (campaign_id,),
        ).fetchone()
        return int(row[0] or 0)

    def _estimated_generation_rate(
        self,
        campaign_id: int,
        days: int,
        now: datetime,
    ) -> float:
        start = now - timedelta(days=days)
        row = self.db.conn.execute(
            """SELECT COUNT(DISTINCT gc.id)
               FROM planned_topics pt
               INNER JOIN generated_content gc ON gc.id = pt.content_id
               WHERE pt.campaign_id = ?
                 AND datetime(gc.created_at) >= datetime(?)
                 AND datetime(gc.created_at) <= datetime(?)""",
            (campaign_id, start.isoformat(), now.isoformat()),
        ).fetchone()
        return float(row[0] or 0) / days

    def _recommendation(
        self,
        remaining_topics: list[dict[str, Any]],
        campaign_id: int,
        days: int,
        now: datetime,
    ) -> CampaignRecommendation:
        if not remaining_topics:
            return CampaignRecommendation(
                planned_topic_id=None,
                topic=None,
                angle=None,
                target_date=None,
                content_type=self._recommended_content_type(campaign_id, days, now),
                reason="No remaining planned topics.",
            )

        overdue = [topic for topic in remaining_topics if self._is_overdue(topic.get("target_date"), now)]
        selected = overdue[0] if overdue else remaining_topics[0]
        target_date = selected.get("target_date")
        reason = (
            "Overdue planned topic should be generated first."
            if overdue
            else "Earliest remaining planned topic is next in the campaign plan."
        )
        return CampaignRecommendation(
            planned_topic_id=selected["id"],
            topic=selected.get("topic"),
            angle=selected.get("angle"),
            target_date=target_date,
            content_type=self._recommended_content_type(campaign_id, days, now),
            reason=reason,
        )

    def _recommended_content_type(self, campaign_id: int, days: int, now: datetime) -> str:
        start = now - timedelta(days=days)
        rows = self.db.conn.execute(
            """SELECT gc.content_type
               FROM planned_topics pt
               INNER JOIN generated_content gc ON gc.id = pt.content_id
               WHERE pt.campaign_id = ?
                 AND datetime(gc.created_at) >= datetime(?)
                 AND datetime(gc.created_at) <= datetime(?)""",
            (campaign_id, start.isoformat(), now.isoformat()),
        ).fetchall()
        counts = Counter(row["content_type"] for row in rows if row["content_type"])
        if counts:
            return counts.most_common(1)[0][0]
        return "x_post"

    def _topic_summary(self, topic: dict[str, Any]) -> dict[str, Any]:
        return {
            "planned_topic_id": topic["id"],
            "topic": topic.get("topic"),
            "angle": topic.get("angle"),
            "target_date": topic.get("target_date"),
            "status": topic.get("status"),
            "content_id": topic.get("content_id"),
        }

    def _is_overdue(self, target_date: str | None, now: datetime) -> bool:
        days_until = _days_until(target_date, now)
        return days_until is not None and days_until < 0

    def _effective_days_remaining(self, days_remaining: int | None) -> int:
        if days_remaining is None:
            return 1
        return max(days_remaining, 0)

    def _miss_risk(
        self,
        *,
        remaining_count: int,
        overdue_count: int,
        days_remaining: int | None,
        estimated_rate: float,
        required_rate: float,
    ) -> str:
        if remaining_count == 0:
            return "on_track"
        if days_remaining is not None and days_remaining < 0:
            return "likely_miss"
        if days_remaining == 0:
            return "likely_miss"
        if overdue_count > 0:
            return "at_risk"
        if required_rate > 0 and estimated_rate < required_rate:
            return "likely_miss"
        return "on_track"
