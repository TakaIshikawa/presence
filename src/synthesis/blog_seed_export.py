"""Export reviewable blog draft briefs from resonated social content."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from output.blog_writer import BlogWriter


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MIN_ENGAGEMENT = 10.0
DEFAULT_LIMIT = 10


class BlogSeedExportError(ValueError):
    """Raised when blog seed export arguments or data are invalid."""


@dataclass(frozen=True)
class BlogSeedSource:
    """One selected social content source for a blog brief."""

    content_id: int
    content_type: str
    content: str
    published_url: str | None
    published_at: str | None
    tweet_id: str | None
    auto_quality: str | None
    engagement_score: float | None
    topics: list[dict[str, Any]] = field(default_factory=list)
    knowledge_links: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class BlogSeedBrief:
    """A structured, reviewable blog seed without a generated full post."""

    source_content_ids: list[int]
    source_urls: list[str]
    source_type: str
    topics: list[dict[str, Any]]
    linked_knowledge: list[dict[str, Any]]
    suggested_title: str
    target_angle: str
    outline: list[str]
    source_excerpt: str
    engagement: dict[str, Any]


@dataclass(frozen=True)
class BlogSeedExport:
    """A file-level artifact containing blog seed briefs."""

    artifact_type: str
    generated_at: str
    lookback_days: int
    min_engagement: float
    seeds: list[BlogSeedBrief]


class BlogSeedExporter:
    """Select resonated social content and turn it into blog seed briefs."""

    def __init__(self, db: Any) -> None:
        self.db = db

    def select_sources(
        self,
        *,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        min_engagement: float = DEFAULT_MIN_ENGAGEMENT,
        limit: int = DEFAULT_LIMIT,
    ) -> list[BlogSeedSource]:
        """Return resonated or engagement-qualified X posts/threads."""
        if lookback_days <= 0:
            raise BlogSeedExportError("lookback_days must be positive")
        if limit <= 0:
            return []

        cursor = self.db.conn.execute(
            """SELECT gc.id, gc.content_type, gc.content, gc.published_url,
                      gc.published_at, gc.tweet_id, gc.auto_quality,
                      pe.engagement_score
               FROM generated_content gc
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id
                              ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               WHERE gc.published = 1
                 AND gc.content_type IN ('x_post', 'x_thread')
                 AND COALESCE(gc.published_at, gc.created_at) >= datetime('now', ?)
                 AND (
                     gc.auto_quality = 'resonated'
                     OR COALESCE(pe.engagement_score, 0) >= ?
                 )
               ORDER BY COALESCE(pe.engagement_score, 0) DESC,
                        COALESCE(gc.published_at, gc.created_at) DESC,
                        gc.id DESC
               LIMIT ?""",
            (f"-{lookback_days} days", min_engagement, limit),
        )
        rows = cursor.fetchall()

        sources: list[BlogSeedSource] = []
        seen_content_ids: set[int] = set()
        for row in rows:
            content_id = int(row["id"])
            if content_id in seen_content_ids:
                continue
            seen_content_ids.add(content_id)
            sources.append(
                BlogSeedSource(
                    content_id=content_id,
                    content_type=row["content_type"],
                    content=row["content"],
                    published_url=row["published_url"],
                    published_at=row["published_at"],
                    tweet_id=row["tweet_id"],
                    auto_quality=row["auto_quality"],
                    engagement_score=(
                        float(row["engagement_score"])
                        if row["engagement_score"] is not None
                        else None
                    ),
                    topics=self._topics(content_id),
                    knowledge_links=self._knowledge_links(content_id),
                )
            )
        return sources

    def build_export(
        self,
        *,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        min_engagement: float = DEFAULT_MIN_ENGAGEMENT,
        limit: int = DEFAULT_LIMIT,
    ) -> BlogSeedExport:
        """Build a structured blog seed export artifact."""
        sources = self.select_sources(
            lookback_days=lookback_days,
            min_engagement=min_engagement,
            limit=limit,
        )
        return BlogSeedExport(
            artifact_type="blog_seed_export",
            generated_at=datetime.now(timezone.utc).isoformat(),
            lookback_days=lookback_days,
            min_engagement=min_engagement,
            seeds=[self.build_brief(source) for source in sources],
        )

    def build_brief(self, source: BlogSeedSource) -> BlogSeedBrief:
        """Build one blog brief from a selected source."""
        clean_content = _strip_thread_markers(source.content)
        topic_labels = [topic["topic"] for topic in source.topics if topic.get("topic")]
        primary_topic = topic_labels[0] if topic_labels else "the core lesson"
        hook = _first_sentence(clean_content) or clean_content or "A resonated social post"
        suggested_title = _suggested_title(hook, topic_labels)
        target_angle = _target_angle(source, primary_topic)

        source_urls = []
        if source.published_url:
            source_urls.append(source.published_url)
        source_urls.extend(
            link["source_url"]
            for link in source.knowledge_links
            if link.get("source_url")
        )

        return BlogSeedBrief(
            source_content_ids=[source.content_id],
            source_urls=_dedupe_strings(source_urls),
            source_type=source.content_type,
            topics=source.topics,
            linked_knowledge=source.knowledge_links,
            suggested_title=suggested_title,
            target_angle=target_angle,
            outline=_outline(clean_content, primary_topic, source.knowledge_links),
            source_excerpt=_truncate(clean_content, 600),
            engagement={
                "score": source.engagement_score,
                "auto_quality": source.auto_quality,
                "published_at": source.published_at,
                "tweet_id": source.tweet_id,
            },
        )

    def _topics(self, content_id: int) -> list[dict[str, Any]]:
        cursor = self.db.conn.execute(
            """SELECT topic, subtopic, confidence
               FROM content_topics
               WHERE content_id = ?
               ORDER BY confidence DESC, id ASC""",
            (content_id,),
        )
        return [
            {
                "topic": row["topic"],
                "subtopic": row["subtopic"],
                "confidence": row["confidence"],
            }
            for row in cursor.fetchall()
        ]

    def _knowledge_links(self, content_id: int) -> list[dict[str, Any]]:
        cursor = self.db.conn.execute(
            """SELECT k.id AS knowledge_id, k.source_type, k.source_id,
                      k.source_url, k.author, k.insight, ckl.relevance_score
               FROM content_knowledge_links ckl
               INNER JOIN knowledge k ON k.id = ckl.knowledge_id
               WHERE ckl.content_id = ?
               ORDER BY ckl.relevance_score DESC, ckl.id ASC""",
            (content_id,),
        )
        return [
            {
                "knowledge_id": row["knowledge_id"],
                "source_type": row["source_type"],
                "source_id": row["source_id"],
                "source_url": row["source_url"],
                "author": row["author"],
                "insight": row["insight"],
                "relevance_score": row["relevance_score"],
            }
            for row in cursor.fetchall()
        ]


def export_to_dict(export: BlogSeedExport) -> dict[str, Any]:
    """Return a JSON-safe export mapping."""
    return asdict(export)


def export_to_json(export: BlogSeedExport) -> str:
    """Serialize the export as stable JSON."""
    return json.dumps(export_to_dict(export), ensure_ascii=False, indent=2, sort_keys=True)


def format_export_markdown(export: BlogSeedExport) -> str:
    """Render the export as Markdown while preserving all artifact data."""
    lines = [
        "# Blog Seed Export",
        "",
        f"- Artifact type: {export.artifact_type}",
        f"- Generated at: {export.generated_at}",
        f"- Lookback days: {export.lookback_days}",
        f"- Minimum engagement: {export.min_engagement}",
        "",
    ]

    if not export.seeds:
        lines.append("No blog seeds selected.")
        return "\n".join(lines).rstrip() + "\n"

    for index, seed in enumerate(export.seeds, start=1):
        lines.extend(
            [
                f"## Seed {index}: {seed.suggested_title}",
                "",
                f"- Source content IDs: {', '.join(str(item) for item in seed.source_content_ids)}",
                f"- Source type: {seed.source_type}",
                f"- Target angle: {seed.target_angle}",
                f"- Engagement score: {seed.engagement.get('score')}",
                f"- Auto quality: {seed.engagement.get('auto_quality')}",
                f"- Published at: {seed.engagement.get('published_at')}",
                f"- Tweet ID: {seed.engagement.get('tweet_id')}",
                "",
                "### Source URLs",
            ]
        )
        lines.extend(f"- {url}" for url in seed.source_urls) if seed.source_urls else lines.append("- none")

        lines.extend(["", "### Topics"])
        if seed.topics:
            lines.extend(
                "- {topic} | {subtopic} | {confidence}".format(
                    topic=topic.get("topic") or "",
                    subtopic=topic.get("subtopic") or "",
                    confidence=topic.get("confidence"),
                )
                for topic in seed.topics
            )
        else:
            lines.append("- none")

        lines.extend(["", "### Linked Knowledge"])
        if seed.linked_knowledge:
            lines.extend(
                "- {knowledge_id} | {source_type} | {source_id} | {source_url} | {author} | {relevance_score} | {insight}".format(
                    knowledge_id=link.get("knowledge_id"),
                    source_type=link.get("source_type") or "",
                    source_id=link.get("source_id") or "",
                    source_url=link.get("source_url") or "",
                    author=link.get("author") or "",
                    relevance_score=link.get("relevance_score"),
                    insight=link.get("insight") or "",
                )
                for link in seed.linked_knowledge
            )
        else:
            lines.append("- none")

        lines.extend(["", "### Outline"])
        lines.extend(f"{item_index}. {item}" for item_index, item in enumerate(seed.outline, start=1))
        lines.extend(["", "### Source Excerpt", "", seed.source_excerpt, ""])

    return "\n".join(lines).rstrip() + "\n"


def write_export(
    export: BlogSeedExport,
    path: str | Path,
    *,
    artifact_format: str = "json",
) -> Path:
    """Write a blog seed export in JSON or Markdown format."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if artifact_format == "json":
        body = export_to_json(export) + "\n"
    elif artifact_format == "markdown":
        body = format_export_markdown(export)
    else:
        raise BlogSeedExportError("artifact_format must be 'json' or 'markdown'")
    target.write_text(body, encoding="utf-8")
    return target


def default_export_filename(export: BlogSeedExport, *, artifact_format: str = "json") -> str:
    """Return a stable default filename for an export artifact."""
    extension = "json" if artifact_format == "json" else "md"
    if export.seeds:
        slug_source = export.seeds[0].suggested_title
    else:
        slug_source = "blog-seeds"
    slug = BlogWriter("/tmp")._slugify(slug_source) or "blog-seeds"
    return f"blog-seeds-{slug}.{extension}"


def _suggested_title(hook: str, topics: list[str]) -> str:
    topic_prefix = topics[0].replace("-", " ").title() if topics else "Engineering"
    hook_text = _truncate(hook, 72).rstrip(".!?")
    return f"{topic_prefix}: {hook_text}"


def _target_angle(source: BlogSeedSource, primary_topic: str) -> str:
    qualifier = "audience response"
    if source.auto_quality == "resonated":
        qualifier = "confirmed resonance"
    elif source.engagement_score is not None:
        qualifier = f"{source.engagement_score:g} engagement score"
    return (
        f"Use {qualifier} on {primary_topic} as the proof point, then expand the short "
        "post into a practical lesson with source-backed context."
    )


def _outline(content: str, primary_topic: str, knowledge_links: list[dict[str, Any]]) -> list[str]:
    hook = _truncate(_first_sentence(content) or content or "the original post", 110).rstrip(".!?")
    support = "Bring in the linked knowledge as supporting evidence."
    if knowledge_links:
        support = "Connect the linked knowledge to the claim without overstating the source."
    return [
        f"Open with the social post's core claim: {hook}.",
        f"Explain why this matters for {primary_topic}.",
        support,
        "Add the implementation or decision context that did not fit in the short-form post.",
        "Close with a reusable takeaway the reader can apply.",
    ]


def _strip_thread_markers(content: str) -> str:
    content = re.sub(r"(?im)^\s*TWEET\s+\d+\s*:\s*", "", content)
    return re.sub(r"\s+", " ", content).strip()


def _first_sentence(text: str) -> str:
    parts = re.split(r"(?<=[.!?])\s+|\n+", text.strip())
    return next((part.strip() for part in parts if part.strip()), "")


def _truncate(text: str, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(text)).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped
