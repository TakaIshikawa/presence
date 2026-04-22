"""Detect gaps between planned topics, generated content, and source activity."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any

from evaluation.topic_extractor import TOPIC_TAXONOMY


TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "architecture": ("architecture", "architectural", "design", "boundary", "module", "service"),
    "testing": ("test", "tests", "testing", "pytest", "fixture", "coverage", "assert"),
    "debugging": ("debug", "debugging", "bug", "fix", "trace", "diagnose", "regression"),
    "ai-agents": ("agent", "agents", "claude", "llm", "prompt", "model", "tool call"),
    "developer-tools": ("cli", "tool", "tools", "script", "workflow command", "dev tool"),
    "performance": ("performance", "latency", "speed", "cache", "optimize", "slow", "throughput"),
    "data-modeling": ("schema", "database", "sqlite", "model", "migration", "table"),
    "devops": ("deploy", "ci", "pipeline", "cron", "infra", "docker", "release"),
    "open-source": ("open source", "oss", "contributor", "license", "repository"),
    "product-thinking": ("product", "user", "ux", "customer", "roadmap", "feature"),
    "workflow": ("workflow", "process", "automation", "handoff", "review", "routine"),
}


@dataclass(frozen=True)
class PlannedTopicGap:
    planned_topic_id: int
    topic: str
    angle: str | None
    target_date: str | None
    campaign_id: int | None
    campaign_name: str | None
    nearest_generated_at: str | None
    days_from_target: int | None


@dataclass(frozen=True)
class OverusedTopic:
    topic: str
    count: int
    share: float
    latest_generated_at: str | None


@dataclass(frozen=True)
class SourceRichGap:
    topic: str
    source_count: int
    commit_count: int
    message_count: int
    latest_source_at: str | None
    latest_generated_at: str | None
    examples: list[str]


@dataclass(frozen=True)
class ContentGapReport:
    period_start: str
    period_end: str
    days: int
    campaign_id: int | None
    planned_gaps: list[PlannedTopicGap]
    overused_topics: list[OverusedTopic]
    source_rich_gaps: list[SourceRichGap]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_datetime(value: str | None) -> datetime | None:
    """Parse SQLite/ISO date values into UTC-aware datetimes."""
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_target_date(value: str | None) -> datetime | None:
    parsed = parse_datetime(value)
    if parsed is not None:
        return parsed
    if not value:
        return None
    try:
        date_value = datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None
    return datetime.combine(date_value, time.min, tzinfo=timezone.utc)


def classify_source_topics(text: str) -> list[str]:
    """Classify source activity with deterministic taxonomy keyword matching."""
    normalized = f" {re.sub(r'[^a-z0-9+#.-]+', ' ', (text or '').lower())} "
    matches: list[tuple[str, int]] = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        score = 0
        for keyword in keywords:
            if " " in keyword or len(keyword) <= 3:
                matched = f" {keyword} " in normalized
            else:
                matched = f" {keyword} " in normalized or keyword in normalized
            if matched:
                score += 1
        if score:
            matches.append((topic, score))
    matches.sort(key=lambda item: (-item[1], TOPIC_TAXONOMY.index(item[0])))
    return [topic for topic, _score in matches[:2]]


class ContentGapDetector:
    """Compare the content calendar against generated output and source activity."""

    def __init__(
        self,
        db,
        *,
        overuse_min_count: int = 3,
        overuse_share: float = 0.45,
        source_rich_min_count: int = 2,
    ):
        self.db = db
        self.overuse_min_count = overuse_min_count
        self.overuse_share = overuse_share
        self.source_rich_min_count = source_rich_min_count

    def detect(
        self,
        *,
        days: int = 14,
        campaign_id: int | None = None,
        target_date: datetime | None = None,
    ) -> ContentGapReport:
        if days <= 0:
            raise ValueError("days must be positive")

        period_end = (target_date or datetime.now(timezone.utc)).astimezone(timezone.utc)
        period_start = period_end - timedelta(days=days)
        generated = self._generated_topic_rows(period_start, period_end)

        return ContentGapReport(
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            days=days,
            campaign_id=campaign_id,
            planned_gaps=self._planned_gaps(generated, days, campaign_id),
            overused_topics=self._overused_topics(generated),
            source_rich_gaps=self._source_rich_gaps(generated, period_start, period_end),
        )

    def _generated_topic_rows(
        self,
        period_start: datetime,
        period_end: datetime,
    ) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            """SELECT gc.id AS content_id,
                      gc.created_at,
                      gc.published_at,
                      ct.topic,
                      ct.subtopic,
                      ct.confidence
               FROM generated_content gc
               INNER JOIN content_topics ct ON ct.content_id = gc.id
               WHERE gc.published != -1
               ORDER BY COALESCE(gc.published_at, gc.created_at) DESC, gc.id DESC"""
        ).fetchall()
        generated = []
        for row in rows:
            item = dict(row)
            generated_at = parse_datetime(item.get("published_at")) or parse_datetime(item.get("created_at"))
            if generated_at is None or generated_at < period_start or generated_at > period_end:
                continue
            item["generated_at"] = generated_at
            generated.append(item)
        return generated

    def _planned_gaps(
        self,
        generated: list[dict[str, Any]],
        days: int,
        campaign_id: int | None,
    ) -> list[PlannedTopicGap]:
        sql = """SELECT pt.*,
                        cc.name AS campaign_name
                 FROM planned_topics pt
                 LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
                 WHERE pt.status = 'planned'"""
        params: list[Any] = []
        if campaign_id is not None:
            sql += " AND pt.campaign_id = ?"
            params.append(campaign_id)
        sql += " ORDER BY pt.target_date ASC NULLS LAST, pt.id ASC"

        plans = [dict(row) for row in self.db.conn.execute(sql, params).fetchall()]
        by_topic: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in generated:
            by_topic[item["topic"]].append(item)

        gaps: list[PlannedTopicGap] = []
        for plan in plans:
            target = parse_target_date(plan.get("target_date"))
            matching = by_topic.get(plan["topic"], [])
            nearest = self._nearest_generated(target, matching)
            delta_days = None
            if target is not None and nearest is not None:
                delta_days = abs((nearest["generated_at"] - target).days)

            has_near_content = False
            if target is None:
                has_near_content = bool(matching)
            elif delta_days is not None:
                has_near_content = delta_days <= days

            if has_near_content:
                continue

            gaps.append(
                PlannedTopicGap(
                    planned_topic_id=plan["id"],
                    topic=plan["topic"],
                    angle=plan.get("angle"),
                    target_date=plan.get("target_date"),
                    campaign_id=plan.get("campaign_id"),
                    campaign_name=plan.get("campaign_name"),
                    nearest_generated_at=nearest["generated_at"].isoformat() if nearest else None,
                    days_from_target=delta_days,
                )
            )
        return gaps

    def _nearest_generated(
        self,
        target: datetime | None,
        matching: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if not matching:
            return None
        if target is None:
            return max(matching, key=lambda item: item["generated_at"])
        return min(matching, key=lambda item: abs(item["generated_at"] - target))

    def _overused_topics(self, generated: list[dict[str, Any]]) -> list[OverusedTopic]:
        counts = Counter(item["topic"] for item in generated if item.get("topic") != "other")
        total = sum(counts.values())
        if total == 0:
            return []

        latest_by_topic: dict[str, datetime] = {}
        for item in generated:
            topic = item["topic"]
            latest_by_topic[topic] = max(item["generated_at"], latest_by_topic.get(topic, item["generated_at"]))

        overused = []
        for topic, count in counts.most_common():
            share = count / total
            if count < self.overuse_min_count or share < self.overuse_share:
                continue
            overused.append(
                OverusedTopic(
                    topic=topic,
                    count=count,
                    share=round(share, 3),
                    latest_generated_at=latest_by_topic[topic].isoformat(),
                )
            )
        return overused

    def _source_rich_gaps(
        self,
        generated: list[dict[str, Any]],
        period_start: datetime,
        period_end: datetime,
    ) -> list[SourceRichGap]:
        source_counts: dict[str, Counter] = defaultdict(Counter)
        latest_source: dict[str, datetime] = {}
        examples: dict[str, list[str]] = defaultdict(list)

        for source_type, row in self._source_rows(period_start, period_end):
            text = row["commit_message"] if source_type == "commit" else row["prompt_text"]
            timestamp = parse_datetime(row["timestamp"])
            if timestamp is None:
                continue
            for topic in classify_source_topics(text):
                source_counts[topic][source_type] += 1
                latest_source[topic] = max(timestamp, latest_source.get(topic, timestamp))
                if len(examples[topic]) < 3:
                    examples[topic].append(text)

        latest_generated: dict[str, datetime] = {}
        for item in generated:
            topic = item["topic"]
            latest_generated[topic] = max(item["generated_at"], latest_generated.get(topic, item["generated_at"]))

        gaps: list[SourceRichGap] = []
        for topic, counts in source_counts.items():
            source_count = counts["commit"] + counts["message"]
            if source_count < self.source_rich_min_count or topic in latest_generated:
                continue
            gaps.append(
                SourceRichGap(
                    topic=topic,
                    source_count=source_count,
                    commit_count=counts["commit"],
                    message_count=counts["message"],
                    latest_source_at=latest_source[topic].isoformat(),
                    latest_generated_at=None,
                    examples=examples[topic],
                )
            )

        return sorted(gaps, key=lambda gap: (-gap.source_count, gap.topic))

    def _source_rows(
        self,
        period_start: datetime,
        period_end: datetime,
    ) -> list[tuple[str, dict[str, Any]]]:
        sources: list[tuple[str, dict[str, Any]]] = []
        for row in self.db.conn.execute("SELECT * FROM github_commits ORDER BY timestamp DESC").fetchall():
            item = dict(row)
            timestamp = parse_datetime(item.get("timestamp"))
            if timestamp and period_start <= timestamp <= period_end:
                sources.append(("commit", item))
        for row in self.db.conn.execute("SELECT * FROM claude_messages ORDER BY timestamp DESC").fetchall():
            item = dict(row)
            timestamp = parse_datetime(item.get("timestamp"))
            if timestamp and period_start <= timestamp <= period_end:
                sources.append(("message", item))
        return sources


def report_to_dict(report: ContentGapReport) -> dict[str, Any]:
    return report.to_dict()
