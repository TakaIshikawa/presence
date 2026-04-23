"""Topic-level engagement history for evaluator calibration."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass

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


@dataclass(frozen=True)
class TopicPerformanceReport:
    """Topic performance query result and applied filters."""

    days: int
    platform: str
    content_type: str | None
    requested_topics: list[str]
    valid_topics: list[str]
    invalid_topics: list[str]
    min_samples: int
    rows: list[TopicPerformance]


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

    def build_topic_performance_report(
        self,
        topics: list[str] | None = None,
        days: int = 90,
        content_type: str | None = None,
        platform: str = "all",
        min_samples: int = 1,
    ) -> TopicPerformanceReport:
        """Return topic performance rows plus the filters applied."""
        normalized_platform = platform.lower() if platform else "all"
        if normalized_platform not in {"all", "x", "bluesky"}:
            raise ValueError("platform must be one of: all, x, bluesky")

        requested_topics = list(dict.fromkeys(topics or []))
        valid_topics, invalid_topics = normalize_topic_filters(requested_topics)
        rows = []
        if requested_topics and not valid_topics:
            return TopicPerformanceReport(
                days=days,
                platform=normalized_platform,
                content_type=content_type,
                requested_topics=requested_topics,
                valid_topics=[],
                invalid_topics=invalid_topics,
                min_samples=min_samples,
                rows=[],
            )

        rows = self._fetch_topic_performance(
            topics=valid_topics if requested_topics else None,
            days=days,
            content_type=content_type,
            platform=normalized_platform,
            min_samples=min_samples,
        )
        return TopicPerformanceReport(
            days=days,
            platform=normalized_platform,
            content_type=content_type,
            requested_topics=requested_topics,
            valid_topics=valid_topics,
            invalid_topics=invalid_topics,
            min_samples=min_samples,
            rows=rows,
        )

    def get_topic_performance(
        self,
        topics: list[str] | None = None,
        days: int = 90,
        content_type: str | None = None,
        platform: str = "all",
        min_samples: int = 1,
    ) -> list[TopicPerformance]:
        """Return recent engagement performance grouped by topic."""
        return self.build_topic_performance_report(
            topics=topics,
            days=days,
            content_type=content_type,
            platform=platform,
            min_samples=min_samples,
        ).rows

    def _fetch_topic_performance(
        self,
        topics: list[str] | None = None,
        days: int = 90,
        content_type: str | None = None,
        platform: str = "all",
        min_samples: int = 1,
    ) -> list[TopicPerformance]:
        """Query recent engagement performance grouped by topic."""
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

        valid_topics, _invalid_topics = normalize_topic_filters(topics)
        if topics is not None and not valid_topics:
            return []
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


def normalize_topic_filters(topics: list[str] | None) -> tuple[list[str], list[str]]:
    """Split requested topics into valid taxonomy topics and ignored entries."""
    valid: list[str] = []
    invalid: list[str] = []
    seen: set[str] = set()

    for topic in topics or []:
        if topic in seen:
            continue
        seen.add(topic)
        if topic in TOPIC_TAXONOMY and topic != "other":
            valid.append(topic)
        else:
            invalid.append(topic)

    return valid, invalid


def topic_performance_report_to_dict(report: TopicPerformanceReport) -> dict[str, object]:
    """Serialize a topic performance report for JSON output."""
    return {
        "status": "ok" if report.rows else "empty",
        "days": report.days,
        "platform": report.platform,
        "content_type": report.content_type,
        "requested_topics": report.requested_topics,
        "valid_topics": report.valid_topics,
        "invalid_topics": report.invalid_topics,
        "min_samples": report.min_samples,
        "row_count": len(report.rows),
        "rows": [asdict(row) for row in report.rows],
    }


def format_topic_performance_json(report: TopicPerformanceReport) -> str:
    """Format a topic performance report as JSON."""
    return json.dumps(topic_performance_report_to_dict(report), indent=2, sort_keys=True)


def format_topic_performance_table(report: TopicPerformanceReport) -> str:
    """Format a topic performance report as a stable text table."""
    lines = [
        "Topic Performance Report",
        "=" * 70,
        f"Lookback:    last {report.days} days",
        f"Platform:    {report.platform}",
        f"Min samples: {report.min_samples}",
    ]
    if report.content_type:
        lines.append(f"Content type: {report.content_type}")
    if report.requested_topics:
        lines.append(f"Requested topics: {', '.join(report.requested_topics)}")
    if report.valid_topics and report.valid_topics != report.requested_topics:
        lines.append(f"Valid topics:     {', '.join(report.valid_topics)}")
    if report.invalid_topics:
        lines.append(f"Ignored topics:   {', '.join(report.invalid_topics)}")
    lines.append("")

    if not report.rows:
        lines.append("No topic performance rows matched the requested filters.")
        return "\n".join(lines)

    headers = [
        "Topic",
        "Samples",
        "Avg Eng",
        "Resonated",
        "Low Res",
        "Latest Published",
    ]
    rendered_rows = [
        [
            row.topic,
            str(row.sample_count),
            f"{row.avg_engagement:.2f}",
            str(row.resonated_count),
            str(row.low_resonance_count),
            row.latest_published_at or "n/a",
        ]
        for row in report.rows
    ]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rendered_rows))
        for index in range(len(headers))
    ]
    lines.append(
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    )
    lines.append("  ".join("-" * width for width in widths))
    for row in rendered_rows:
        lines.append(
            "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))
        )
    return "\n".join(lines)
