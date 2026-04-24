"""Build read-only generation briefs for upcoming campaign topics."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from synthesis.content_gaps import classify_source_topics, parse_datetime


@dataclass(frozen=True)
class CampaignBriefEvidence:
    """One supporting item for a planned topic."""

    source_type: str
    source_id: str
    title: str
    excerpt: str
    timestamp: str | None = None
    url: str | None = None
    relevance: str = "related"


@dataclass(frozen=True)
class CampaignBriefTopic:
    """Generation context for one planned topic."""

    planned_topic_id: int
    topic: str
    angle: str | None
    target_date: str | None
    source_material: str | None
    evidence: list[CampaignBriefEvidence]
    knowledge_snippets: list[CampaignBriefEvidence]
    previous_related_posts: list[CampaignBriefEvidence]
    risks: list[str]


@dataclass(frozen=True)
class CampaignBrief:
    """A concise read-only planning artifact for campaign generation."""

    campaign: dict[str, Any] | None
    generated_at: str
    limit: int
    topics: list[CampaignBriefTopic]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class CampaignBriefBuilder:
    """Assemble campaign topic briefs without mutating planning or content state."""

    def __init__(
        self,
        db,
        *,
        source_days: int = 30,
        similar_days: int = 45,
        max_evidence_per_type: int = 3,
    ):
        self.db = db
        self.source_days = source_days
        self.similar_days = similar_days
        self.max_evidence_per_type = max_evidence_per_type

    def build(
        self,
        *,
        campaign_id: int | None = None,
        limit: int = 3,
        now: datetime | None = None,
    ) -> CampaignBrief:
        """Build a brief for the next planned topics in a campaign."""
        if limit <= 0:
            raise ValueError("limit must be positive")

        generated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        campaign = self._campaign(campaign_id)
        resolved_campaign_id = campaign["id"] if campaign else campaign_id
        topics = self._planned_topics(resolved_campaign_id, limit)
        recent_posts = self._recent_posts(generated_at)

        brief_topics = []
        for topic in topics:
            evidence = self._supporting_evidence(topic, generated_at)
            knowledge = self._knowledge_snippets(topic)
            previous_posts = self._previous_related_posts(topic, recent_posts)
            brief_topics.append(
                CampaignBriefTopic(
                    planned_topic_id=topic["id"],
                    topic=topic["topic"],
                    angle=topic.get("angle"),
                    target_date=topic.get("target_date"),
                    source_material=topic.get("source_material"),
                    evidence=evidence,
                    knowledge_snippets=knowledge,
                    previous_related_posts=previous_posts,
                    risks=self._risks(topic, recent_posts, evidence, knowledge),
                )
            )

        return CampaignBrief(
            campaign=campaign,
            generated_at=generated_at.isoformat(),
            limit=limit,
            topics=brief_topics,
        )

    def _campaign(self, campaign_id: int | None) -> dict[str, Any] | None:
        if campaign_id is not None:
            return self.db.get_campaign(campaign_id)
        if hasattr(self.db, "get_active_campaign"):
            return self.db.get_active_campaign()
        return None

    def _planned_topics(self, campaign_id: int | None, limit: int) -> list[dict[str, Any]]:
        sql = """SELECT pt.*,
                        cc.name AS campaign_name,
                        cc.goal AS campaign_goal
                 FROM planned_topics pt
                 LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
                 WHERE pt.status = 'planned'"""
        params: list[Any] = []
        if campaign_id is not None:
            sql += " AND pt.campaign_id = ?"
            params.append(campaign_id)
        sql += " ORDER BY pt.target_date ASC NULLS LAST, pt.created_at ASC, pt.id ASC LIMIT ?"
        params.append(limit)
        return [dict(row) for row in self.db.conn.execute(sql, params).fetchall()]

    def _supporting_evidence(
        self,
        topic: dict[str, Any],
        generated_at: datetime,
    ) -> list[CampaignBriefEvidence]:
        period_start = generated_at - timedelta(days=self.source_days)
        explicit = self._source_material_refs(topic.get("source_material"))
        evidence: list[CampaignBriefEvidence] = []

        for row in self.db.conn.execute(
            "SELECT * FROM github_commits ORDER BY timestamp DESC, id DESC"
        ).fetchall():
            item = dict(row)
            timestamp = parse_datetime(item.get("timestamp"))
            is_explicit = item.get("commit_sha") in explicit
            if not is_explicit and not self._in_window(timestamp, period_start, generated_at):
                continue
            if not is_explicit and not self._row_matches_topic(
                topic, item.get("commit_message", "")
            ):
                continue
            evidence.append(
                CampaignBriefEvidence(
                    source_type="commit",
                    source_id=item["commit_sha"],
                    title=f"{item.get('repo_name') or 'repo'}@{item['commit_sha'][:8]}",
                    excerpt=self._shorten(item.get("commit_message")),
                    timestamp=item.get("timestamp"),
                    relevance="explicit" if is_explicit else "topic_match",
                )
            )
            if len([item for item in evidence if item.source_type == "commit"]) >= self.max_evidence_per_type:
                break

        for row in self.db.conn.execute(
            "SELECT * FROM claude_messages ORDER BY timestamp DESC, id DESC"
        ).fetchall():
            item = dict(row)
            timestamp = parse_datetime(item.get("timestamp"))
            refs = {str(item.get("message_uuid")), str(item.get("session_id"))}
            is_explicit = bool(refs & explicit)
            if not is_explicit and not self._in_window(timestamp, period_start, generated_at):
                continue
            if not is_explicit and not self._row_matches_topic(topic, item.get("prompt_text", "")):
                continue
            evidence.append(
                CampaignBriefEvidence(
                    source_type="session",
                    source_id=item["message_uuid"],
                    title=f"Session {item.get('session_id') or 'unknown'}",
                    excerpt=self._shorten(item.get("prompt_text")),
                    timestamp=item.get("timestamp"),
                    relevance="explicit" if is_explicit else "topic_match",
                )
            )
            if len([item for item in evidence if item.source_type == "session"]) >= self.max_evidence_per_type:
                break

        return evidence

    def _knowledge_snippets(self, topic: dict[str, Any]) -> list[CampaignBriefEvidence]:
        snippets: list[CampaignBriefEvidence] = []
        rows = self.db.conn.execute(
            """SELECT * FROM knowledge
               WHERE approved = 1
               ORDER BY COALESCE(published_at, ingested_at, created_at) DESC, id DESC"""
        ).fetchall()
        for row in rows:
            item = dict(row)
            text = " ".join(
                value for value in [item.get("insight"), item.get("content")] if value
            )
            if not self._row_matches_topic(topic, text):
                continue
            snippets.append(
                CampaignBriefEvidence(
                    source_type="knowledge",
                    source_id=str(item["id"]),
                    title=f"{item.get('source_type') or 'knowledge'}"
                    + (f" by {item['author']}" if item.get("author") else ""),
                    excerpt=self._shorten(item.get("insight") or item.get("content")),
                    timestamp=item.get("published_at") or item.get("ingested_at") or item.get("created_at"),
                    url=item.get("source_url"),
                    relevance="topic_match",
                )
            )
            if len(snippets) >= self.max_evidence_per_type:
                break
        return snippets

    def _recent_posts(self, generated_at: datetime) -> list[dict[str, Any]]:
        period_start = generated_at - timedelta(days=self.similar_days)
        rows = self.db.conn.execute(
            """SELECT gc.id AS content_id,
                      gc.content_type,
                      gc.content,
                      gc.content_format,
                      gc.eval_score,
                      gc.created_at,
                      gc.published_at,
                      ct.topic,
                      ct.subtopic
               FROM generated_content gc
               LEFT JOIN content_topics ct ON ct.content_id = gc.id
               WHERE gc.published != -1
               ORDER BY COALESCE(gc.published_at, gc.created_at) DESC, gc.id DESC"""
        ).fetchall()
        posts = []
        for row in rows:
            item = dict(row)
            timestamp = parse_datetime(item.get("published_at")) or parse_datetime(item.get("created_at"))
            if timestamp is None or timestamp < period_start or timestamp > generated_at:
                continue
            item["generated_at"] = timestamp
            posts.append(item)
        return posts

    def _previous_related_posts(
        self,
        topic: dict[str, Any],
        recent_posts: list[dict[str, Any]],
    ) -> list[CampaignBriefEvidence]:
        posts: list[CampaignBriefEvidence] = []
        seen: set[int] = set()
        for item in recent_posts:
            content_id = item["content_id"]
            if content_id in seen:
                continue
            if item.get("topic") != topic["topic"] and not self._row_matches_topic(
                topic, item.get("content", "")
            ):
                continue
            seen.add(content_id)
            posts.append(
                CampaignBriefEvidence(
                    source_type="previous_post",
                    source_id=str(content_id),
                    title=f"#{content_id} {item.get('content_type') or 'content'}",
                    excerpt=self._shorten(item.get("content")),
                    timestamp=(item.get("published_at") or item.get("created_at")),
                    relevance="same_topic" if item.get("topic") == topic["topic"] else "text_match",
                )
            )
            if len(posts) >= self.max_evidence_per_type:
                break
        return posts

    def _risks(
        self,
        topic: dict[str, Any],
        recent_posts: list[dict[str, Any]],
        evidence: list[CampaignBriefEvidence],
        knowledge: list[CampaignBriefEvidence],
    ) -> list[str]:
        risks: list[str] = []
        same_topic = [
            item for item in recent_posts
            if item.get("topic") == topic["topic"]
            or self._row_matches_topic(topic, item.get("content", ""))
        ]
        if same_topic:
            latest = max(item["generated_at"] for item in same_topic)
            risks.append(
                f"recent similar content: {len({item['content_id'] for item in same_topic})} "
                f"related item(s) in the last {self.similar_days} days, latest {latest.date().isoformat()}"
            )

        formats = Counter(
            item.get("content_format")
            for item in same_topic
            if item.get("content_format")
        )
        if formats:
            content_format, count = formats.most_common(1)[0]
            if count >= 2:
                risks.append(f"overused pattern: {content_format} appeared {count} times recently")

        if not evidence and not knowledge:
            risks.append("thin evidence: no recent commits, sessions, or approved knowledge matched")
        return risks

    def _row_matches_topic(self, topic: dict[str, Any], text: str | None) -> bool:
        haystack = (text or "").lower()
        if not haystack:
            return False
        topic_name = (topic.get("topic") or "").lower()
        if topic_name and topic_name in classify_source_topics(haystack):
            return True
        for token in self._query_tokens(topic):
            if token in haystack:
                return True
        return False

    def _query_tokens(self, topic: dict[str, Any]) -> set[str]:
        text = " ".join(
            str(value)
            for value in [topic.get("topic"), topic.get("angle"), topic.get("campaign_goal")]
            if value
        ).lower()
        return {
            token
            for token in re.findall(r"[a-z0-9][a-z0-9+#.-]{2,}", text)
            if token not in {"the", "and", "for", "with", "from", "into", "about"}
        }

    def _source_material_refs(self, value: str | None) -> set[str]:
        if not value:
            return set()
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return set(re.findall(r"[A-Za-z0-9_:/.-]{4,}", str(value)))
        refs: set[str] = set()
        if isinstance(parsed, dict):
            values = parsed.values()
        elif isinstance(parsed, list):
            values = parsed
        else:
            values = [parsed]
        for item in values:
            if isinstance(item, list):
                refs.update(str(value) for value in item)
            elif isinstance(item, dict):
                refs.update(str(value) for value in item.values())
            elif item is not None:
                refs.add(str(item))
        return refs

    @staticmethod
    def _in_window(
        timestamp: datetime | None,
        period_start: datetime,
        period_end: datetime,
    ) -> bool:
        return timestamp is not None and period_start <= timestamp <= period_end

    @staticmethod
    def _shorten(text: str | None, width: int = 180) -> str:
        value = " ".join((text or "").split())
        if len(value) <= width:
            return value
        return value[: max(0, width - 3)] + "..."


def brief_to_dict(brief: CampaignBrief) -> dict[str, Any]:
    return brief.to_dict()
