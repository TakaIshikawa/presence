"""Recommend topics for the next newsletter from gaps and available content."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from evaluation.newsletter_source_mix import parse_source_content_ids


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 10


@dataclass(frozen=True)
class NewsletterTopicRecommendation:
    """One topic section recommendation for newsletter assembly."""

    topic: str
    recommendation_type: str
    reason: str
    supporting_content_ids: tuple[int, ...]
    supporting_planned_topic_ids: tuple[int, ...]
    recent_newsletter_uses: int
    available_content_count: int
    open_planned_topic_count: int
    newest_content_at: str | None
    newest_planned_at: str | None
    last_newsletter_sent_at: str | None
    freshness_days: int | None
    campaign_ids: tuple[int, ...]
    campaign_names: tuple[str, ...]
    score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class NewsletterTopicPlanner:
    """Build forward-looking newsletter topic recommendations."""

    def __init__(self, db: Any) -> None:
        self.db = db
        self.conn = getattr(db, "conn", db)

    def recommend(
        self,
        *,
        days: int = DEFAULT_DAYS,
        limit: int = DEFAULT_LIMIT,
        include_planned: bool = False,
        now: datetime | None = None,
    ) -> list[NewsletterTopicRecommendation]:
        """Return ranked topics to consider for the next newsletter."""
        now = _ensure_utc(now or datetime.now(timezone.utc))
        cutoff = now - timedelta(days=days)

        sent_content_ids, usage_by_topic, last_sent_by_topic = self._recent_usage(cutoff)
        available_by_topic = self._available_content(cutoff, sent_content_ids)
        planned_by_topic = self._open_planned_topics() if include_planned else {}

        topics = sorted(set(available_by_topic) | set(planned_by_topic))
        recommendations = [
            self._build_recommendation(
                topic=topic,
                usage_count=usage_by_topic.get(topic, 0),
                last_sent_at=last_sent_by_topic.get(topic),
                content_rows=available_by_topic.get(topic, []),
                planned_rows=planned_by_topic.get(topic, []),
                now=now,
            )
            for topic in topics
        ]

        recommendations.sort(
            key=lambda item: (
                -item.score,
                item.recent_newsletter_uses,
                item.topic,
            )
        )
        return recommendations[: max(0, limit)]

    def _recent_usage(
        self, cutoff: datetime
    ) -> tuple[set[int], Counter[str], dict[str, str]]:
        if not _has_tables(self.conn, {"newsletter_sends", "content_topics"}):
            return set(), Counter(), {}

        rows = self.conn.execute(
            """SELECT id, source_content_ids, sent_at
               FROM newsletter_sends
               WHERE sent_at >= ?
                 AND COALESCE(status, 'sent') != 'draft'
               ORDER BY sent_at DESC, id DESC""",
            (_to_iso(cutoff),),
        ).fetchall()

        sent_content_ids: set[int] = set()
        sent_at_by_content: dict[int, str] = {}
        for row in rows:
            source_ids, _warnings = parse_source_content_ids(row["source_content_ids"])
            sent_at = row["sent_at"] or ""
            for content_id in source_ids:
                sent_content_ids.add(content_id)
                if sent_at and (
                    content_id not in sent_at_by_content
                    or sent_at > sent_at_by_content[content_id]
                ):
                    sent_at_by_content[content_id] = sent_at

        if not sent_content_ids:
            return sent_content_ids, Counter(), {}

        topic_rows = self._topic_rows(sorted(sent_content_ids))
        usage_by_topic: Counter[str] = Counter()
        last_sent_by_topic: dict[str, str] = {}
        seen_pairs: set[tuple[int, str]] = set()
        for row in topic_rows:
            content_id = int(row["content_id"])
            topic = row["topic"]
            if not topic or (content_id, topic) in seen_pairs:
                continue
            seen_pairs.add((content_id, topic))
            usage_by_topic[topic] += 1
            sent_at = sent_at_by_content.get(content_id)
            if sent_at and (
                topic not in last_sent_by_topic or sent_at > last_sent_by_topic[topic]
            ):
                last_sent_by_topic[topic] = sent_at

        return sent_content_ids, usage_by_topic, last_sent_by_topic

    def _available_content(
        self, cutoff: datetime, sent_content_ids: set[int]
    ) -> dict[str, list[dict[str, Any]]]:
        if not _has_tables(self.conn, {"generated_content", "content_topics"}):
            return {}

        rows = self.conn.execute(
            """SELECT gc.id, gc.created_at, gc.published_at, gc.content_type,
                      ct.topic, ct.confidence
               FROM generated_content gc
               INNER JOIN content_topics ct ON ct.content_id = gc.id
               WHERE gc.created_at >= ?
                 AND COALESCE(gc.published, 0) != -1
               ORDER BY gc.created_at DESC, gc.id DESC, ct.confidence DESC""",
            (_to_iso(cutoff),),
        ).fetchall()

        by_topic: dict[str, list[dict[str, Any]]] = defaultdict(list)
        seen_pairs: set[tuple[str, int]] = set()
        for row in rows:
            content_id = int(row["id"])
            topic = row["topic"]
            if not topic or content_id in sent_content_ids:
                continue
            key = (topic, content_id)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            by_topic[topic].append(dict(row))
        return dict(by_topic)

    def _open_planned_topics(self) -> dict[str, list[dict[str, Any]]]:
        if not _has_tables(self.conn, {"planned_topics"}):
            return {}

        has_campaigns = _has_tables(self.conn, {"content_campaigns"})
        if has_campaigns:
            sql = """SELECT pt.id, pt.topic, pt.angle, pt.target_date, pt.created_at,
                            pt.campaign_id, cc.name AS campaign_name,
                            cc.status AS campaign_status
                     FROM planned_topics pt
                     LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
                     WHERE pt.status = 'planned'
                     ORDER BY pt.target_date ASC NULLS LAST, pt.created_at ASC"""
        else:
            sql = """SELECT pt.id, pt.topic, pt.angle, pt.target_date, pt.created_at,
                            pt.campaign_id, NULL AS campaign_name,
                            NULL AS campaign_status
                     FROM planned_topics pt
                     WHERE pt.status = 'planned'
                     ORDER BY pt.target_date ASC NULLS LAST, pt.created_at ASC"""

        by_topic: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in self.conn.execute(sql).fetchall():
            if row["topic"]:
                by_topic[row["topic"]].append(dict(row))
        return dict(by_topic)

    def _topic_rows(self, content_ids: list[int]) -> list[Any]:
        if not content_ids:
            return []
        placeholders = ",".join("?" for _ in content_ids)
        return self.conn.execute(
            f"""SELECT content_id, topic
                FROM content_topics
                WHERE content_id IN ({placeholders})
                ORDER BY content_id, topic""",
            content_ids,
        ).fetchall()

    def _build_recommendation(
        self,
        *,
        topic: str,
        usage_count: int,
        last_sent_at: str | None,
        content_rows: list[dict[str, Any]],
        planned_rows: list[dict[str, Any]],
        now: datetime,
    ) -> NewsletterTopicRecommendation:
        supporting_content_ids = tuple(
            int(row["id"]) for row in sorted(
                content_rows,
                key=lambda row: (row.get("created_at") or "", int(row["id"])),
                reverse=True,
            )[:5]
        )
        supporting_planned_topic_ids = tuple(
            int(row["id"]) for row in sorted(
                planned_rows,
                key=lambda row: (row.get("target_date") or "9999", row.get("created_at") or ""),
            )[:5]
        )
        campaign_ids = tuple(
            sorted(
                {
                    int(row["campaign_id"])
                    for row in planned_rows
                    if row.get("campaign_id") is not None
                }
            )
        )
        campaign_names = tuple(
            sorted(
                {
                    str(row["campaign_name"])
                    for row in planned_rows
                    if row.get("campaign_name")
                }
            )
        )

        newest_content_at = _max_text(
            row.get("created_at") or row.get("published_at") for row in content_rows
        )
        newest_planned_at = _max_text(
            row.get("target_date") or row.get("created_at") for row in planned_rows
        )
        freshness_source = _max_text([newest_content_at, newest_planned_at])
        freshness_days = _age_days(freshness_source, now)

        recommendation_type = _recommendation_type(
            usage_count=usage_count,
            content_count=len(content_rows),
            planned_rows=planned_rows,
        )
        reason = _reason(
            recommendation_type=recommendation_type,
            topic=topic,
            usage_count=usage_count,
            content_count=len(content_rows),
            planned_count=len(planned_rows),
            last_sent_at=last_sent_at,
        )
        score = _score(
            recommendation_type=recommendation_type,
            usage_count=usage_count,
            content_count=len(content_rows),
            planned_count=len(planned_rows),
            freshness_days=freshness_days,
        )

        return NewsletterTopicRecommendation(
            topic=topic,
            recommendation_type=recommendation_type,
            reason=reason,
            supporting_content_ids=supporting_content_ids,
            supporting_planned_topic_ids=supporting_planned_topic_ids,
            recent_newsletter_uses=usage_count,
            available_content_count=len(content_rows),
            open_planned_topic_count=len(planned_rows),
            newest_content_at=newest_content_at,
            newest_planned_at=newest_planned_at,
            last_newsletter_sent_at=last_sent_at,
            freshness_days=freshness_days,
            campaign_ids=campaign_ids,
            campaign_names=campaign_names,
            score=score,
        )


def build_newsletter_topic_plan(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    include_planned: bool = False,
    now: datetime | None = None,
) -> list[NewsletterTopicRecommendation]:
    """Convenience wrapper for callers and scripts."""
    return NewsletterTopicPlanner(db).recommend(
        days=days,
        limit=limit,
        include_planned=include_planned,
        now=now,
    )


def format_newsletter_topic_plan_json(
    recommendations: list[NewsletterTopicRecommendation],
) -> str:
    """Serialize recommendations as stable JSON."""
    return json.dumps(
        [recommendation.to_dict() for recommendation in recommendations],
        indent=2,
        sort_keys=True,
    )


def _recommendation_type(
    *,
    usage_count: int,
    content_count: int,
    planned_rows: list[dict[str, Any]],
) -> str:
    if any(row.get("campaign_id") is not None for row in planned_rows):
        return "campaign-backed"
    if usage_count == 0:
        return "underused"
    if content_count > 0:
        return "newly-available"
    return "underused"


def _reason(
    *,
    recommendation_type: str,
    topic: str,
    usage_count: int,
    content_count: int,
    planned_count: int,
    last_sent_at: str | None,
) -> str:
    inventory = []
    if content_count:
        inventory.append(f"{content_count} unsent content item{'s' if content_count != 1 else ''}")
    if planned_count:
        inventory.append(f"{planned_count} open planned topic{'s' if planned_count != 1 else ''}")
    inventory_text = " and ".join(inventory) or "available inventory"

    if recommendation_type == "campaign-backed":
        return f"{topic} is backed by an open campaign plan with {inventory_text}."
    if usage_count == 0:
        return f"{topic} has not appeared in recent newsletter sources and has {inventory_text}."
    last_seen = f" Last newsletter use: {last_sent_at}." if last_sent_at else ""
    return (
        f"{topic} has {inventory_text} after {usage_count} recent newsletter "
        f"use{'s' if usage_count != 1 else ''}.{last_seen}"
    )


def _score(
    *,
    recommendation_type: str,
    usage_count: int,
    content_count: int,
    planned_count: int,
    freshness_days: int | None,
) -> float:
    type_bonus = {
        "campaign-backed": 5.0,
        "underused": 3.0,
        "newly-available": 2.0,
    }.get(recommendation_type, 0.0)
    freshness_bonus = 2.0 if freshness_days is None else max(0.0, 2.0 - freshness_days / 14)
    return round(type_bonus + content_count * 1.5 + planned_count - usage_count + freshness_bonus, 2)


def _has_tables(conn: Any, table_names: set[str]) -> bool:
    placeholders = ",".join("?" for _ in table_names)
    rows = conn.execute(
        f"""SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name IN ({placeholders})""",
        sorted(table_names),
    ).fetchall()
    return {row["name"] for row in rows} == table_names


def _max_text(values: Any) -> str | None:
    present = [value for value in values if value]
    return max(present) if present else None


def _age_days(value: str | None, now: datetime) -> int | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return max(0, (now - parsed).days)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    candidate = value
    if len(candidate) == 10:
        candidate = f"{candidate}T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_iso(value: datetime) -> str:
    return _ensure_utc(value).isoformat()
