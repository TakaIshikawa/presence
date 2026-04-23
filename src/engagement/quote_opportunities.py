"""Recommend curated source items worth quote-posting."""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from typing import Any

from knowledge.source_scorer import SourceScorer
from storage.db import IntegrityError
from synthesis.content_gaps import ContentGapDetector, classify_source_topics, parse_datetime


CURATED_SOURCE_TYPES = ("curated_x", "curated_article", "curated_newsletter")
QUOTEABLE_SOURCE_TYPES = ("curated_x", "curated_article", "curated_newsletter")
TOKEN_RE = re.compile(r"[a-z0-9+#.-]+")


@dataclass(frozen=True)
class QuoteOpportunity:
    knowledge_id: int
    source_type: str
    source_id: str | None
    source_url: str | None
    author: str | None
    content: str
    insight: str | None
    published_at: str | None
    campaign_id: int | None
    campaign_name: str | None
    topics: list[str]
    score: float
    topical_relevance: float
    freshness: float
    source_quality: float
    novelty: float
    prior_performance: float
    reasons: list[str]
    draft_text: str
    already_enqueued: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _tokens(text: str | None) -> set[str]:
    return {
        token
        for token in TOKEN_RE.findall((text or "").lower())
        if len(token) > 2 and token not in {"the", "and", "for", "with", "that", "this"}
    }


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _source_key(row: dict[str, Any]) -> str:
    return str(row.get("source_url") or row.get("source_id") or row.get("id"))


def _target_tweet_id(row: dict[str, Any]) -> str:
    if row.get("source_type") == "curated_x" and row.get("source_id"):
        return str(row["source_id"])
    key = _source_key(row)
    return f"source:{sha1(key.encode('utf-8')).hexdigest()[:20]}"


def _normalize_author(author: str | None) -> str | None:
    if not author:
        return None
    return author.strip().lstrip("@").lower()


def _freshness_score(published_at: str | None, now: datetime, half_life_days: float) -> float:
    timestamp = parse_datetime(published_at)
    if timestamp is None:
        return 0.25
    age_days = max((now - timestamp).total_seconds(), 0) / 86400
    return _clamp(math.pow(0.5, age_days / half_life_days))


class QuoteOpportunityRecommender:
    """Score recent curated source items for quote-post review."""

    def __init__(
        self,
        db,
        *,
        source_scorer: SourceScorer | None = None,
        freshness_half_life_days: float = 3.0,
    ) -> None:
        self.db = db
        self.source_scorer = source_scorer or SourceScorer(db)
        self.freshness_half_life_days = freshness_half_life_days

    def recommend(
        self,
        *,
        days: int = 7,
        limit: int = 10,
        campaign_id: int | None = None,
        min_score: float = 0.35,
        authors: list[str] | None = None,
        topics: list[str] | None = None,
        source_types: list[str] | None = None,
        now: datetime | None = None,
    ) -> list[QuoteOpportunity]:
        if days <= 0:
            raise ValueError("days must be positive")
        if limit <= 0:
            raise ValueError("limit must be positive")

        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        cutoff = now - timedelta(days=days)
        normalized_authors = {
            author
            for author in (_normalize_author(value) for value in (authors or []))
            if author
        }
        normalized_topics = {str(topic).strip().lower() for topic in (topics or []) if str(topic).strip()}
        normalized_source_types = {
            str(source_type).strip()
            for source_type in (source_types or [])
            if str(source_type).strip()
        }
        campaign_rows = self._campaign_rows(campaign_id, now)
        if campaign_id is not None and not campaign_rows:
            raise ValueError(f"Campaign {campaign_id} is not active or does not exist")

        campaign_topics = self._campaign_topics([row["id"] for row in campaign_rows])
        gap_topics = self._gap_topics(days, campaign_id, now)
        source_quality = self._source_quality_scores()
        topic_performance = self._topic_performance_scores()
        recent_post_tokens = [_tokens(row["content"]) for row in self.db.get_recent_published_content_all(limit=30)]
        used_source_keys = self._used_source_keys()

        opportunities: list[QuoteOpportunity] = []
        for row in self._candidate_rows(
            cutoff,
            authors=normalized_authors or None,
            topics=normalized_topics or None,
            source_types=normalized_source_types or None,
        ):
            if _source_key(row) in used_source_keys:
                continue
            if self.db.proactive_action_exists(_target_tweet_id(row), "quote_tweet"):
                continue

            opportunity = self._score_row(
                row,
                now=now,
                campaign_rows=campaign_rows,
                campaign_topics=campaign_topics,
                gap_topics=gap_topics,
                source_quality=source_quality,
                topic_performance=topic_performance,
                recent_post_tokens=recent_post_tokens,
            )
            if opportunity.score >= min_score:
                opportunities.append(opportunity)

        opportunities.sort(key=lambda item: (-item.score, item.published_at or "", item.knowledge_id))
        return opportunities[:limit]

    def enqueue(
        self,
        opportunities: list[QuoteOpportunity],
        *,
        limit: int | None = None,
    ) -> list[int]:
        inserted: list[int] = []
        for opportunity in opportunities[: limit or len(opportunities)]:
            metadata = {
                "kind": "quote_opportunity",
                "knowledge_id": opportunity.knowledge_id,
                "source_type": opportunity.source_type,
                "source_url": opportunity.source_url,
                "source_id": opportunity.source_id,
                "topics": opportunity.topics,
                "score_components": {
                    "topical_relevance": opportunity.topical_relevance,
                    "freshness": opportunity.freshness,
                    "source_quality": opportunity.source_quality,
                    "novelty": opportunity.novelty,
                    "prior_performance": opportunity.prior_performance,
                },
                "reasons": opportunity.reasons,
            }
            try:
                action_id = self.db.insert_proactive_action(
                    action_type="quote_tweet",
                    target_tweet_id=_target_tweet_id(opportunity.to_dict()),
                    target_tweet_text=opportunity.content,
                    target_author_handle=opportunity.author or "",
                    discovery_source="quote_opportunities",
                    relevance_score=opportunity.score,
                    draft_text=opportunity.draft_text,
                    knowledge_ids=json.dumps([[opportunity.knowledge_id, opportunity.score]]),
                    platform_metadata=json.dumps(metadata, sort_keys=True),
                )
            except IntegrityError:
                continue
            inserted.append(action_id)
        return inserted

    def _score_row(
        self,
        row: dict[str, Any],
        *,
        now: datetime,
        campaign_rows: list[dict[str, Any]],
        campaign_topics: dict[int, set[str]],
        gap_topics: set[str],
        source_quality: dict[tuple[str | None, str], float],
        topic_performance: dict[str, float],
        recent_post_tokens: list[set[str]],
    ) -> QuoteOpportunity:
        text = " ".join(str(row.get(key) or "") for key in ("content", "insight", "author"))
        topics = classify_source_topics(text)
        topic_set = set(topics)

        campaign_id, campaign_name, topical_relevance = self._campaign_match(
            topic_set,
            text,
            campaign_rows,
            campaign_topics,
        )
        if topic_set & gap_topics:
            topical_relevance = max(topical_relevance, 0.85)

        freshness = _freshness_score(
            row.get("published_at") or row.get("ingested_at") or row.get("created_at"),
            now,
            self.freshness_half_life_days,
        )
        author_key = _normalize_author(row.get("author"))
        source_type = str(row.get("source_type") or "")
        quality = source_quality.get((author_key, source_type), 0.45)
        prior_performance = max((topic_performance.get(topic, 0.4) for topic in topics), default=0.4)
        novelty = self._novelty(row.get("content"), recent_post_tokens)

        score = (
            0.34 * topical_relevance
            + 0.20 * freshness
            + 0.18 * quality
            + 0.18 * novelty
            + 0.10 * prior_performance
        )
        reasons = self._reasons(
            topics=topics,
            gap_topics=gap_topics,
            campaign_name=campaign_name,
            freshness=freshness,
            quality=quality,
            novelty=novelty,
        )
        return QuoteOpportunity(
            knowledge_id=int(row["id"]),
            source_type=source_type,
            source_id=row.get("source_id"),
            source_url=row.get("source_url"),
            author=row.get("author"),
            content=row.get("content") or "",
            insight=row.get("insight"),
            published_at=row.get("published_at") or row.get("ingested_at") or row.get("created_at"),
            campaign_id=campaign_id,
            campaign_name=campaign_name,
            topics=topics,
            score=round(score, 4),
            topical_relevance=round(topical_relevance, 4),
            freshness=round(freshness, 4),
            source_quality=round(quality, 4),
            novelty=round(novelty, 4),
            prior_performance=round(prior_performance, 4),
            reasons=reasons,
            draft_text=self._draft_text(row, topics, campaign_name),
        )

    def _candidate_rows(
        self,
        cutoff: datetime,
        *,
        authors: set[str] | None = None,
        topics: set[str] | None = None,
        source_types: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            f"""SELECT *
                FROM knowledge
                WHERE source_type IN ({",".join("?" for _ in QUOTEABLE_SOURCE_TYPES)})
                  AND approved = 1
                ORDER BY COALESCE(published_at, ingested_at, created_at) DESC, id DESC""",
            QUOTEABLE_SOURCE_TYPES,
        ).fetchall()
        candidates: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            timestamp = parse_datetime(item.get("published_at") or item.get("ingested_at") or item.get("created_at"))
            if timestamp is None or timestamp >= cutoff:
                author_key = _normalize_author(item.get("author"))
                source_type = str(item.get("source_type") or "")
                if authors and author_key not in authors:
                    continue
                if source_types and source_type not in source_types:
                    continue
                if topics:
                    text = " ".join(str(item.get(key) or "") for key in ("content", "insight", "author"))
                    if not (set(classify_source_topics(text)) & topics):
                        continue
                candidates.append(item)
        return candidates

    def _campaign_rows(self, campaign_id: int | None, now: datetime) -> list[dict[str, Any]]:
        today = now.date().isoformat()
        if campaign_id is not None:
            row = self.db.get_campaign(campaign_id)
            if (
                row
                and row.get("status") == "active"
                and (row.get("start_date") is None or row.get("start_date") <= today)
                and (row.get("end_date") is None or row.get("end_date") >= today)
            ):
                return [row]
            return []

        rows = self.db.conn.execute(
            """SELECT *
               FROM content_campaigns
               WHERE status = 'active'
                 AND (start_date IS NULL OR start_date <= ?)
                 AND (end_date IS NULL OR end_date >= ?)
               ORDER BY start_date DESC NULLS LAST, created_at DESC""",
            (today, today),
        ).fetchall()
        return [dict(row) for row in rows]

    def _campaign_topics(self, campaign_ids: list[int]) -> dict[int, set[str]]:
        if not campaign_ids:
            return {}
        placeholders = ",".join("?" for _ in campaign_ids)
        rows = self.db.conn.execute(
            f"""SELECT campaign_id, topic
                FROM planned_topics
                WHERE status = 'planned'
                  AND campaign_id IN ({placeholders})""",
            campaign_ids,
        ).fetchall()
        topics: dict[int, set[str]] = {}
        for row in rows:
            topics.setdefault(row["campaign_id"], set()).add(row["topic"])
        return topics

    def _campaign_match(
        self,
        topics: set[str],
        text: str,
        campaign_rows: list[dict[str, Any]],
        campaign_topics: dict[int, set[str]],
    ) -> tuple[int | None, str | None, float]:
        if not campaign_rows:
            return None, None, 0.35 if topics else 0.15

        best: tuple[int | None, str | None, float] = (None, None, 0.2)
        text_tokens = _tokens(text)
        for campaign in campaign_rows:
            planned = campaign_topics.get(campaign["id"], set())
            topic_score = 0.0
            if topics and planned:
                topic_score = len(topics & planned) / len(topics | planned)
            goal_score = _jaccard(text_tokens, _tokens(f"{campaign.get('name')} {campaign.get('goal')}"))
            score = max(topic_score, goal_score * 0.8)
            if score > best[2]:
                best = (campaign["id"], campaign.get("name"), score)
        return best

    def _gap_topics(self, days: int, campaign_id: int | None, now: datetime) -> set[str]:
        report = ContentGapDetector(self.db).detect(days=days, campaign_id=campaign_id, target_date=now)
        return {gap.topic for gap in report.planned_gaps} | {gap.topic for gap in report.source_rich_gaps}

    def _source_quality_scores(self) -> dict[tuple[str | None, str], float]:
        scores = self.source_scorer.compute_scores(days=90, min_uses=1)
        if not scores:
            return {}
        max_score = max(score.quality_score for score in scores) or 1.0
        return {
            (_normalize_author(score.author), score.source_type): _clamp(score.quality_score / max_score)
            for score in scores
        }

    def _topic_performance_scores(self) -> dict[str, float]:
        rows = self.db.conn.execute(
            """SELECT ct.topic, AVG(COALESCE(pe.engagement_score, 0)) AS avg_engagement
               FROM content_topics ct
               INNER JOIN generated_content gc ON gc.id = ct.content_id
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE gc.published = 1
               GROUP BY ct.topic"""
        ).fetchall()
        values = {row["topic"]: float(row["avg_engagement"] or 0.0) for row in rows}
        if not values:
            return {}
        max_value = max(values.values()) or 1.0
        return {topic: _clamp(value / max_value) for topic, value in values.items()}

    def _used_source_keys(self) -> set[str]:
        used = {
            str(row["source_key"])
            for row in self.db.conn.execute(
                """SELECT DISTINCT COALESCE(k.source_url, k.source_id, CAST(k.id AS TEXT)) AS source_key
                   FROM knowledge k
                   INNER JOIN content_knowledge_links ckl ON ckl.knowledge_id = k.id
                   INNER JOIN generated_content gc ON gc.id = ckl.content_id
                   WHERE gc.published != -1"""
            ).fetchall()
            if row["source_key"]
        }
        rows = self.db.conn.execute(
            """SELECT platform_metadata
               FROM proactive_actions
               WHERE action_type = 'quote_tweet'
                 AND status IN ('pending', 'approved', 'posted')"""
        ).fetchall()
        for row in rows:
            try:
                metadata = json.loads(row["platform_metadata"] or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            for key in ("source_url", "source_id"):
                if metadata.get(key):
                    used.add(str(metadata[key]))
        return used

    def _novelty(self, content: str | None, recent_post_tokens: list[set[str]]) -> float:
        candidate = _tokens(content)
        if not candidate or not recent_post_tokens:
            return 0.75
        max_overlap = max((_jaccard(candidate, tokens) for tokens in recent_post_tokens), default=0.0)
        return _clamp(1.0 - max_overlap)

    def _reasons(
        self,
        *,
        topics: list[str],
        gap_topics: set[str],
        campaign_name: str | None,
        freshness: float,
        quality: float,
        novelty: float,
    ) -> list[str]:
        reasons: list[str] = []
        if campaign_name:
            reasons.append(f"matches active campaign {campaign_name}")
        matched_gaps = sorted(set(topics) & gap_topics)
        if matched_gaps:
            reasons.append("fills gap: " + ", ".join(matched_gaps))
        if freshness >= 0.75:
            reasons.append("fresh source item")
        if quality >= 0.65:
            reasons.append("strong source history")
        if novelty >= 0.75:
            reasons.append("novel versus recent posts")
        return reasons or ["general curated-source fit"]

    def _draft_text(self, row: dict[str, Any], topics: list[str], campaign_name: str | None) -> str:
        topic = topics[0] if topics else "this"
        author = row.get("author") or "this source"
        base = row.get("insight") or row.get("content") or ""
        excerpt = " ".join(str(base).split())[:180]
        context = f" for {campaign_name}" if campaign_name else ""
        return f"Quote-post angle{context}: connect @{str(author).lstrip('@')}'s point to {topic}. Draft note: {excerpt}"


def opportunities_to_dict(opportunities: list[QuoteOpportunity]) -> list[dict[str, Any]]:
    return [opportunity.to_dict() for opportunity in opportunities]
