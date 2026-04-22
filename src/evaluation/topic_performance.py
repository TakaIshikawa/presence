"""Topic-level engagement history for evaluator calibration."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from evaluation.topic_extractor import TOPIC_TAXONOMY

logger = logging.getLogger(__name__)


TOPIC_KEYWORDS = {
    "architecture": ["architecture", "design", "boundary", "interface", "system"],
    "testing": ["test", "testing", "pytest", "fixture", "coverage", "regression", "dry run"],
    "debugging": ["debug", "debugging", "bug", "error", "trace", "failure", "fix"],
    "ai-agents": ["agent", "claude", "llm", "prompt", "model", "ai"],
    "developer-tools": ["tool", "cli", "workflow", "editor", "automation"],
    "performance": ["performance", "latency", "slow", "fast", "optimize"],
    "data-modeling": ["schema", "database", "model", "migration", "sqlite"],
    "devops": ["deploy", "ci", "cron", "pipeline", "release", "ops"],
    "open-source": ["open source", "github", "pr", "issue", "maintainer"],
    "product-thinking": ["product", "user", "customer", "feedback", "value"],
    "workflow": ["workflow", "process", "handoff", "review", "routine"],
}


@dataclass(frozen=True)
class TopicPerformance:
    """Engagement summary for one topic."""

    topic: str
    sample_count: int
    avg_engagement: float
    resonated_count: int
    low_resonance_count: int
    latest_published_at: str | None = None


class TopicPerformanceAnalyzer:
    """Builds compact topic engagement notes from existing history tables."""

    def __init__(self, db, topic_extractor=None) -> None:
        self.db = db
        self.topic_extractor = topic_extractor

    def infer_topics(self, texts: list[str], max_topics: int = 3) -> list[str]:
        """Infer likely taxonomy topics from text.

        If a TopicExtractor is supplied, use its classifier. Otherwise use a
        conservative keyword fallback so this calibration remains optional.
        """
        joined = "\n".join(t for t in texts if t).strip()
        if not joined:
            return []

        if self.topic_extractor:
            try:
                extracted = self.topic_extractor.extract_topics(joined[:6000])
                topics = [
                    topic
                    for topic, _subtopic, confidence in extracted
                    if topic in TOPIC_TAXONOMY and topic != "other" and confidence >= 0.4
                ]
                if topics:
                    return list(dict.fromkeys(topics))[:max_topics]
            except Exception as e:
                logger.debug("Topic extraction failed; using keyword fallback: %s", e)

        lowered = joined.lower()
        scored: list[tuple[str, int]] = []
        for topic, keywords in TOPIC_KEYWORDS.items():
            score = sum(
                1
                for keyword in keywords
                if re.search(rf"\b{re.escape(keyword)}\b", lowered)
            )
            if score:
                scored.append((topic, score))

        scored.sort(key=lambda item: (-item[1], item[0]))
        return [topic for topic, _score in scored[:max_topics]]

    def get_topic_performance(
        self,
        topics: list[str] | None = None,
        days: int = 90,
        content_type: str | None = None,
        platform: str = "all",
        min_samples: int = 1,
    ) -> list[TopicPerformance]:
        """Return recent engagement performance grouped by topic."""
        normalized_platform = platform.lower() if platform else "all"
        if normalized_platform not in {"all", "x", "bluesky"}:
            raise ValueError("platform must be one of: all, x, bluesky")

        clauses = [
            "published = 1",
            "published_at >= datetime('now', ?)",
            "confidence >= 0.4",
            "engagement_score IS NOT NULL",
        ]
        params: list = [f"-{days} days"]

        if content_type:
            clauses.append("content_type = ?")
            params.append(content_type)

        valid_topics = [
            topic for topic in (topics or []) if topic in TOPIC_TAXONOMY and topic != "other"
        ]
        if valid_topics:
            placeholders = ", ".join("?" for _ in valid_topics)
            clauses.append(f"topic IN ({placeholders})")
            params.extend(valid_topics)

        where_sql = " AND ".join(clauses)
        if normalized_platform == "x":
            engagement_expr = "lx.engagement_score"
        elif normalized_platform == "bluesky":
            engagement_expr = "lb.engagement_score"
        else:
            engagement_expr = (
                "CASE WHEN lx.engagement_score IS NOT NULL "
                "OR lb.engagement_score IS NOT NULL "
                "THEN COALESCE(lx.engagement_score, 0) + COALESCE(lb.engagement_score, 0) "
                "END"
            )

        cursor = self.db.conn.execute(
            f"""WITH latest_x AS (
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
               ),
               topic_rows AS (
                   SELECT ct.topic,
                          ct.confidence,
                          gc.content_type,
                          gc.published,
                          gc.auto_quality,
                          gc.published_at,
                          {engagement_expr} AS engagement_score
                   FROM content_topics ct
                   INNER JOIN generated_content gc ON gc.id = ct.content_id
                   LEFT JOIN latest_x lx ON lx.content_id = gc.id
                   LEFT JOIN latest_bluesky lb ON lb.content_id = gc.id
               )
               SELECT topic,
                      COUNT(*) AS sample_count,
                      AVG(engagement_score) AS avg_engagement,
                      SUM(CASE WHEN auto_quality = 'resonated' THEN 1 ELSE 0 END)
                          AS resonated_count,
                      SUM(CASE WHEN auto_quality = 'low_resonance' THEN 1 ELSE 0 END)
                          AS low_resonance_count,
                      MAX(published_at) AS latest_published_at
               FROM topic_rows
               WHERE {where_sql}
               GROUP BY topic
               HAVING sample_count >= ?
               ORDER BY avg_engagement DESC, sample_count DESC, topic ASC""",
            (*params, min_samples),
        )

        return [
            TopicPerformance(
                topic=row["topic"],
                sample_count=row["sample_count"],
                avg_engagement=round(row["avg_engagement"] or 0.0, 2),
                resonated_count=row["resonated_count"] or 0,
                low_resonance_count=row["low_resonance_count"] or 0,
                latest_published_at=row["latest_published_at"],
            )
            for row in cursor.fetchall()
        ]

    def build_evaluation_context(
        self,
        source_texts: list[str],
        candidate_texts: list[str],
        days: int = 90,
        content_type: str | None = None,
        platform: str = "all",
    ) -> str:
        """Build a compact evaluator prompt block with topic history."""
        focus_topics = self.infer_topics(source_texts + candidate_texts)
        focus = self.get_topic_performance(
            topics=focus_topics,
            days=days,
            content_type=content_type,
            platform=platform,
        )
        overall = self.get_topic_performance(
            days=days,
            content_type=content_type,
            platform=platform,
            min_samples=2,
        )

        if not focus and not overall:
            return ""

        lines = [
            "ENGAGEMENT HISTORY BY TOPIC (calibration signal, not a hard rule):"
        ]
        if focus_topics:
            lines.append(f"- Current/source topics detected: {', '.join(focus_topics)}")
        if focus:
            lines.append("- Recent performance for matching topics:")
            for item in focus[:3]:
                lines.append(f"  {self._format_topic_line(item)}")

        resonant = [
            item for item in overall if item.sample_count >= 2 and item.avg_engagement > 0
        ][:3]
        low_resonance = sorted(
            [
                item
                for item in overall
                if item.sample_count >= 2 and item.low_resonance_count >= item.resonated_count
            ],
            key=lambda item: (item.avg_engagement, -item.sample_count, item.topic),
        )[:3]

        if resonant:
            lines.append("- Historically resonant topics:")
            for item in resonant:
                lines.append(f"  {self._format_topic_line(item)}")
        if low_resonance:
            lines.append("- Historically low-resonance topics:")
            for item in low_resonance:
                lines.append(f"  {self._format_topic_line(item)}")

        lines.append(
            "Use this as a prior about audience behavior; a strong, specific story can beat topic averages."
        )
        return "\n".join(lines)

    @staticmethod
    def _format_topic_line(item: TopicPerformance) -> str:
        return (
            f"- {item.topic}: n={item.sample_count}, "
            f"avg engagement={item.avg_engagement:.1f}, "
            f"resonated={item.resonated_count}, "
            f"low_resonance={item.low_resonance_count}"
        )
