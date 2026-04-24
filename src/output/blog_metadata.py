"""Export social and canonical metadata for generated blog posts."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class BlogMetadata:
    """Stable metadata payload for a generated blog post."""

    content_id: int
    title: str
    description: str
    canonical_url: str | None
    og_type: str
    image: str | None
    image_alt_text: str | None
    published_at: str | None
    topics: list[str]
    open_graph: dict[str, Any]
    twitter_card: dict[str, Any]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _strip_frontmatter(content: str) -> str:
    if not content.startswith("---"):
        return content
    match = re.match(r"\A---\s*\n.*?\n---\s*\n?", content, flags=re.DOTALL)
    if not match:
        return content
    return content[match.end() :]


def _frontmatter_title(content: str) -> str | None:
    if not content.startswith("---"):
        return None
    match = re.match(r"\A---\s*\n(.*?)\n---", content, flags=re.DOTALL)
    if not match:
        return None
    for line in match.group(1).splitlines():
        key, sep, value = line.partition(":")
        if sep and key.strip() == "title":
            title = value.strip().strip("\"'")
            return title or None
    return None


def _content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:8]


def extract_title_and_body(content: str, content_id: int | None = None) -> tuple[str, str]:
    """Extract a blog title and markdown body, using a deterministic fallback."""
    title_match = re.search(r"^TITLE:\s*(.+)$", content, re.MULTILINE)
    if title_match:
        return title_match.group(1).strip(), content[title_match.end() :].strip()

    frontmatter_title = _frontmatter_title(content)
    body = _strip_frontmatter(content).strip()
    if frontmatter_title:
        return frontmatter_title, body

    h1_match = re.search(r"^#\s+(.+)$", body, re.MULTILINE)
    if h1_match:
        title = h1_match.group(1).strip()
        body = (body[: h1_match.start()] + body[h1_match.end() :]).strip()
        return title, body

    suffix = str(content_id) if content_id is not None else _content_hash(content)
    return f"Blog Post {suffix}", body


def extract_description(content: str, max_length: int = 160) -> str:
    """Use the first prose paragraph as a social preview description."""
    body = _strip_frontmatter(content)
    lines = [line.strip() for line in body.splitlines() if line.strip()]
    for line in lines:
        if line.startswith(("TITLE:", "#", "-", "*", ">")):
            continue
        line = re.sub(r"\*\*(.+?)\*\*", r"\1", line)
        line = re.sub(r"`(.+?)`", r"\1", line)
        if len(line) > max_length:
            return line[: max_length - 3].rstrip() + "..."
        return line
    return ""


def _normalize_topics(rows: list[Any]) -> list[str]:
    topics: list[str] = []
    seen = set()
    for row in rows:
        value = row.get("topic") if isinstance(row, dict) else row["topic"]
        topic = str(value).strip()
        if topic and topic not in seen:
            topics.append(topic)
            seen.add(topic)
    return topics


def _warnings(row: dict[str, Any]) -> list[str]:
    warnings = []
    if not row.get("published_url"):
        warnings.append("missing_canonical_url")
    if row.get("image_path") and not row.get("image_alt_text"):
        warnings.append("missing_image_alt_text")
    return warnings


def metadata_from_row(row: dict[str, Any], topics: list[Any] | None = None) -> BlogMetadata:
    """Build social metadata from one generated_content blog_post row."""
    content_id = int(row["id"])
    title, body = extract_title_and_body(row.get("content") or "", content_id)
    description = extract_description(body)
    canonical_url = row.get("published_url") or None
    image = row.get("image_path") or None
    image_alt_text = row.get("image_alt_text") or None
    published_at = row.get("published_at") or None
    topic_values = _normalize_topics(topics or [])
    warnings = _warnings(row)

    open_graph = {
        "og:title": title,
        "og:description": description,
        "og:type": "article",
        "og:url": canonical_url,
        "og:image": image,
        "og:image:alt": image_alt_text,
        "article:published_time": published_at,
        "article:tag": topic_values,
    }
    twitter_card = {
        "twitter:card": "summary_large_image" if image else "summary",
        "twitter:title": title,
        "twitter:description": description,
        "twitter:image": image,
        "twitter:image:alt": image_alt_text,
    }

    return BlogMetadata(
        content_id=content_id,
        title=title,
        description=description,
        canonical_url=canonical_url,
        og_type="article",
        image=image,
        image_alt_text=image_alt_text,
        published_at=published_at,
        topics=topic_values,
        open_graph=open_graph,
        twitter_card=twitter_card,
        warnings=warnings,
    )


class BlogMetadataExporter:
    """Read-only exporter for generated blog post metadata."""

    def __init__(self, db: Any) -> None:
        self.db = db

    def export_content_id(self, content_id: int) -> BlogMetadata:
        row = self.db.get_generated_content(content_id)
        if not row:
            raise ValueError(f"generated_content id {content_id} not found")
        if row.get("content_type") != "blog_post":
            raise ValueError(f"generated_content id {content_id} is not a blog_post")
        return metadata_from_row(row, self._topics_for_content(content_id))

    def export_recent(self, days: int = 30, now: datetime | None = None) -> list[BlogMetadata]:
        if days <= 0:
            raise ValueError("days must be positive")
        cutoff = (_normalize_now(now) - timedelta(days=days)).isoformat()
        rows = self.db.conn.execute(
            """SELECT *
               FROM generated_content
               WHERE content_type = 'blog_post'
                 AND published = 1
                 AND published_at IS NOT NULL
                 AND datetime(published_at) >= datetime(?)
               ORDER BY datetime(published_at) DESC, id DESC""",
            (cutoff,),
        ).fetchall()
        return [
            metadata_from_row(dict(row), self._topics_for_content(int(row["id"])))
            for row in rows
        ]

    def _topics_for_content(self, content_id: int) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            """SELECT topic
               FROM content_topics
               WHERE content_id = ?
               ORDER BY created_at ASC, id ASC""",
            (content_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def _normalize_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now


def metadata_to_json(metadata: BlogMetadata | list[BlogMetadata]) -> str:
    """Serialize metadata as stable pretty JSON."""
    if isinstance(metadata, list):
        payload: Any = [item.to_dict() for item in metadata]
    else:
        payload = metadata.to_dict()
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def metadata_to_markdown(metadata: BlogMetadata | list[BlogMetadata]) -> str:
    """Serialize metadata as compact Markdown for build logs and review."""
    items = metadata if isinstance(metadata, list) else [metadata]
    if not items:
        return "No blog metadata found.\n"

    sections = []
    for item in items:
        data = item.to_dict()
        lines = [
            f"## {item.title}",
            "",
            f"- content_id: {item.content_id}",
            f"- canonical_url: {item.canonical_url or ''}",
            f"- og_type: {item.og_type}",
            f"- image: {item.image or ''}",
            f"- image_alt_text: {item.image_alt_text or ''}",
            f"- published_at: {item.published_at or ''}",
            f"- topics: {', '.join(item.topics)}",
            f"- warnings: {', '.join(item.warnings)}",
            "",
            "```json",
            json.dumps(data, ensure_ascii=False, indent=2),
            "```",
        ]
        sections.append("\n".join(lines))
    return "\n\n".join(sections) + "\n"
