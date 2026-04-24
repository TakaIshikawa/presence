"""Find historically strong topics that have gone stale."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from synthesis.content_gaps import parse_datetime


@dataclass(frozen=True)
class StaleTopic:
    topic: str
    score: float
    sample_count: int
    avg_engagement: float
    max_engagement: float
    latest_published_at: str
    days_since_latest: int
    source_content_ids: list[int]
    reasons: list[str]


@dataclass(frozen=True)
class StaleTopicReport:
    min_age_days: int
    lookback_days: int
    limit: int
    generated_after: str
    history_after: str
    topics: list[StaleTopic]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class StaleTopicResurfacer:
    """Rank strong historical topics absent from recent generated content."""

    def __init__(
        self,
        db,
        *,
        min_avg_engagement: float = 5.0,
        min_samples: int = 1,
    ) -> None:
        self.db = db
        self.min_avg_engagement = min_avg_engagement
        self.min_samples = min_samples

    def detect(
        self,
        *,
        min_age_days: int = 30,
        lookback_days: int = 180,
        limit: int = 10,
        target_date: datetime | None = None,
    ) -> StaleTopicReport:
        if min_age_days <= 0:
            raise ValueError("min_age_days must be positive")
        if lookback_days <= min_age_days:
            raise ValueError("lookback_days must be greater than min_age_days")
        if limit <= 0:
            return StaleTopicReport(
                min_age_days=min_age_days,
                lookback_days=lookback_days,
                limit=limit,
                generated_after=self._now(target_date).isoformat(),
                history_after=self._now(target_date).isoformat(),
                topics=[],
            )

        now = self._now(target_date)
        recent_cutoff = now - timedelta(days=min_age_days)
        history_cutoff = now - timedelta(days=lookback_days)
        rows = self._topic_content_rows()

        recent_topics = {
            row["topic"]
            for row in rows
            if row["generated_at"] is not None and row["generated_at"] >= recent_cutoff
        }

        by_topic: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            generated_at = row["generated_at"]
            if generated_at is None or generated_at < history_cutoff:
                continue
            if row["topic"] in recent_topics:
                continue
            engagement = row["engagement_score"]
            if engagement is None:
                continue
            by_topic.setdefault(row["topic"], []).append(row)

        topics = [
            self._build_stale_topic(topic, items, now)
            for topic, items in by_topic.items()
        ]
        topics = [
            topic
            for topic in topics
            if topic.sample_count >= self.min_samples
            and topic.avg_engagement >= self.min_avg_engagement
        ]
        topics.sort(
            key=lambda topic: (
                -topic.score,
                -topic.avg_engagement,
                -topic.days_since_latest,
                topic.topic,
            )
        )

        return StaleTopicReport(
            min_age_days=min_age_days,
            lookback_days=lookback_days,
            limit=limit,
            generated_after=recent_cutoff.isoformat(),
            history_after=history_cutoff.isoformat(),
            topics=topics[:limit],
        )

    @staticmethod
    def _now(target_date: datetime | None) -> datetime:
        value = target_date or datetime.now(timezone.utc)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _topic_content_rows(self) -> list[dict[str, Any]]:
        cursor = self.db.conn.execute(
            """WITH latest_x AS (
                   SELECT content_id, engagement_score
                   FROM (
                       SELECT content_id, engagement_score,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id ORDER BY fetched_at DESC
                              ) AS rn
                       FROM post_engagement
                   )
                   WHERE rn = 1
               ),
               latest_bluesky AS (
                   SELECT content_id, engagement_score
                   FROM (
                       SELECT content_id, engagement_score,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id ORDER BY fetched_at DESC
                              ) AS rn
                       FROM bluesky_engagement
                   )
                   WHERE rn = 1
               )
               SELECT ct.topic,
                      gc.id AS content_id,
                      gc.published_at,
                      gc.created_at,
                      CASE WHEN lx.engagement_score IS NOT NULL
                             OR lb.engagement_score IS NOT NULL
                           THEN COALESCE(lx.engagement_score, 0)
                              + COALESCE(lb.engagement_score, 0)
                      END AS engagement_score
               FROM content_topics ct
               INNER JOIN generated_content gc ON gc.id = ct.content_id
               LEFT JOIN latest_x lx ON lx.content_id = gc.id
               LEFT JOIN latest_bluesky lb ON lb.content_id = gc.id
               WHERE gc.published != -1
                 AND ct.confidence >= 0.4
               ORDER BY ct.topic ASC, gc.id ASC"""
        )
        rows: list[dict[str, Any]] = []
        for row in cursor.fetchall():
            item = dict(row)
            item["generated_at"] = parse_datetime(item.get("published_at")) or parse_datetime(
                item.get("created_at")
            )
            rows.append(item)
        return rows

    def _build_stale_topic(
        self,
        topic: str,
        rows: list[dict[str, Any]],
        now: datetime,
    ) -> StaleTopic:
        rows = sorted(
            rows,
            key=lambda row: (
                row["generated_at"] or datetime.min.replace(tzinfo=timezone.utc),
                row["content_id"],
            ),
            reverse=True,
        )
        engagement_scores = [float(row["engagement_score"] or 0.0) for row in rows]
        avg_engagement = sum(engagement_scores) / len(engagement_scores)
        max_engagement = max(engagement_scores)
        latest = rows[0]["generated_at"]
        days_since_latest = max(0, (now.date() - latest.date()).days) if latest else 0
        source_content_ids = [int(row["content_id"]) for row in rows[:5]]
        score = avg_engagement + (max_engagement * 0.25) + min(days_since_latest / 30, 6)

        reasons = [
            f"Average historical engagement {avg_engagement:.1f} across {len(rows)} posts",
            f"Best historical engagement {max_engagement:.1f}",
            f"Last generated {days_since_latest} days ago",
        ]
        return StaleTopic(
            topic=topic,
            score=round(score, 2),
            sample_count=len(rows),
            avg_engagement=round(avg_engagement, 2),
            max_engagement=round(max_engagement, 2),
            latest_published_at=latest.isoformat() if latest else "",
            days_since_latest=days_since_latest,
            source_content_ids=source_content_ids,
            reasons=reasons,
        )


def report_to_dict(report: StaleTopicReport) -> dict[str, Any]:
    return report.to_dict()


def format_stale_topic_json(report: StaleTopicReport) -> str:
    return json.dumps(report_to_dict(report), indent=2, sort_keys=True)
