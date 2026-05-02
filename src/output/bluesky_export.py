"""Build manual Bluesky publishing artifacts from generated content."""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .platform_adapter import BLUESKY_GRAPHEME_LIMIT
from .platform_adapter import count_graphemes
from .platform_adapter import slice_graphemes
from .x_client import parse_thread_content


_URL_RE = re.compile(r"https?://[^\s<>()]+")
_WHITESPACE_RE = re.compile(r"[ \t]+")
_SENTENCE_END_RE = re.compile(r"(?<=[.!?])(?:[\"')\]]+)?\s+")


@dataclass(frozen=True)
class BlueskyExportOptions:
    """Configuration for Bluesky artifact generation."""

    max_length: int = BLUESKY_GRAPHEME_LIMIT
    include_sources: bool = True


@dataclass(frozen=True)
class BlueskySource:
    """A source link that can be carried into a Bluesky manual draft."""

    url: str
    label: str | None = None


@dataclass(frozen=True)
class BlueskyPost:
    """One ordered Bluesky post."""

    index: int
    total: int
    text: str

    @property
    def graphemes(self) -> int:
        return count_graphemes(self.text)


@dataclass(frozen=True)
class BlueskyExport:
    """Bluesky-ready posts plus operator-facing artifact metadata."""

    content_id: int
    content_type: str
    posts: tuple[BlueskyPost, ...]
    sources: tuple[BlueskySource, ...] = ()
    max_length: int = BLUESKY_GRAPHEME_LIMIT
    queue: dict[str, Any] | None = None
    queue_id: int | None = None

    @property
    def post_count(self) -> int:
        return len(self.posts)


class BlueskyExportError(ValueError):
    """Raised when a Bluesky export cannot be built."""


def build_bluesky_export(
    content: dict[str, Any],
    *,
    queue: dict[str, Any] | None = None,
    options: BlueskyExportOptions | None = None,
) -> BlueskyExport:
    """Transform one generated content row into Bluesky-ready manual drafts."""
    options = options or BlueskyExportOptions()
    if options.max_length <= 0:
        raise BlueskyExportError("max_length must be positive")

    if "id" not in content:
        raise BlueskyExportError("content is missing id")

    content_id = int(content["id"])
    content_type = str(content.get("content_type") or "x_post")
    source_text = str(content.get("content") or "").strip()
    if not source_text:
        raise BlueskyExportError(f"generated_content id {content_id} has no content")

    parts = _source_parts_for_bluesky(source_text, content_type)
    if not parts:
        raise BlueskyExportError(f"generated_content id {content_id} has no content")

    sources = _source_attributions(source_text, _embedded_source_rows(content))
    if options.include_sources and sources:
        parts = _append_sources_when_possible(parts, sources, options.max_length)

    post_texts = _split_posts(parts, options.max_length)
    queue_id = queue.get("queue_id") if queue else None
    if queue_id is None and queue:
        queue_id = queue.get("id")

    return BlueskyExport(
        content_id=content_id,
        content_type=content_type,
        posts=tuple(
            BlueskyPost(index=index, total=len(post_texts), text=text)
            for index, text in enumerate(post_texts, start=1)
        ),
        sources=tuple(sources),
        max_length=options.max_length,
        queue=dict(queue) if queue else None,
        queue_id=queue_id,
    )


def build_bluesky_export_from_db(
    db: Any,
    *,
    content_id: int | None = None,
    queue_id: int | None = None,
    options: BlueskyExportOptions | None = None,
) -> BlueskyExport:
    """Fetch generated or queued content and build read-only Bluesky artifact data."""
    if (content_id is None) == (queue_id is None):
        raise ValueError("Pass exactly one of content_id or queue_id")

    queue = None
    if queue_id is not None:
        row = db.conn.execute(
            """SELECT pq.id AS queue_id,
                      pq.content_id AS queue_content_id,
                      pq.scheduled_at,
                      pq.platform,
                      pq.status,
                      pq.published_at,
                      pq.error,
                      pq.error_category,
                      pq.hold_reason,
                      pq.created_at,
                      gc.*
               FROM publish_queue pq
               INNER JOIN generated_content gc ON gc.id = pq.content_id
               WHERE pq.id = ?""",
            (queue_id,),
        ).fetchone()
        if not row:
            raise BlueskyExportError(f"publish_queue id {queue_id} not found")
        content = dict(row)
        queue = {
            "id": content.get("queue_id"),
            "content_id": content.get("queue_content_id"),
            "scheduled_at": content.get("scheduled_at"),
            "platform": content.get("platform"),
            "status": content.get("status"),
            "published_at": content.get("published_at"),
            "error": content.get("error"),
            "error_category": content.get("error_category"),
            "hold_reason": content.get("hold_reason"),
            "created_at": content.get("created_at"),
        }
        content_id = int(content["id"])
    else:
        row = db.conn.execute(
            "SELECT * FROM generated_content WHERE id = ?",
            (content_id,),
        ).fetchone()
        if not row:
            raise BlueskyExportError(f"generated_content id {content_id} not found")
        content = dict(row)

    content["__sources"] = _content_lineage(db, int(content_id))
    return build_bluesky_export(content, queue=queue, options=options)


def write_bluesky_markdown(export: BlueskyExport, path: str | Path) -> Path:
    """Write a markdown artifact for manual Bluesky publishing."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(format_bluesky_markdown(export), encoding="utf-8")
    return target


def format_bluesky_markdown(export: BlueskyExport) -> str:
    """Render an operator-facing Bluesky markdown artifact."""
    lines = [
        "# Bluesky Draft",
        "",
        f"- Content ID: {export.content_id}",
        f"- Content type: {export.content_type}",
        f"- Posts: {export.post_count}",
        f"- Limit: {export.max_length} graphemes",
    ]
    if export.queue_id is not None:
        lines.append(f"- Queue ID: {export.queue_id}")
    if export.queue:
        if export.queue.get("scheduled_at"):
            lines.append(f"- Scheduled at: {export.queue['scheduled_at']}")
        if export.queue.get("platform"):
            lines.append(f"- Platform: {export.queue['platform']}")
        if export.queue.get("status"):
            lines.append(f"- Queue status: {export.queue['status']}")
        if export.queue.get("published_at"):
            lines.append(f"- Published at: {export.queue['published_at']}")
        if export.queue.get("error"):
            lines.append(f"- Queue error: {export.queue['error']}")
        if export.queue.get("error_category"):
            lines.append(f"- Error category: {export.queue['error_category']}")
        if export.queue.get("hold_reason"):
            lines.append(f"- Hold reason: {export.queue['hold_reason']}")

    lines.extend(["", "## Posts", ""])
    for post in export.posts:
        if export.post_count > 1:
            lines.append(f"### Post {post.index}/{post.total}")
            lines.append("")
        lines.append(post.text)
        lines.append("")
        lines.append(f"_Length: {post.graphemes}/{export.max_length} graphemes_")
        lines.append("")

    if export.sources:
        lines.extend(["## Sources", ""])
        for source in export.sources:
            label = source.label or "Source"
            lines.append(f"- {label}: {source.url}")

    return "\n".join(lines).rstrip() + "\n"


def bluesky_export_to_dict(export: BlueskyExport) -> dict[str, Any]:
    """Return a JSON-safe dictionary for a Bluesky export."""
    data = asdict(export)
    data["post_count"] = export.post_count
    for post in data["posts"]:
        post["graphemes"] = count_graphemes(post["text"])
    return data


def bluesky_export_to_json(export: BlueskyExport) -> str:
    """Serialize a Bluesky export as stable JSON."""
    return json.dumps(bluesky_export_to_dict(export), indent=2, sort_keys=True)


def _source_parts_for_bluesky(text: str, content_type: str) -> list[str]:
    if content_type == "x_thread":
        return [_normalize_spacing(part) for part in parse_thread_content(text)]
    normalized = _normalize_spacing(text)
    return [normalized] if normalized else []


def _split_posts(parts: list[str], max_length: int) -> list[str]:
    posts: list[str] = []
    for part in parts:
        posts.extend(_split_body(part, max_length))
    return posts


def _split_body(text: str, max_length: int) -> list[str]:
    if count_graphemes(text) <= max_length:
        return [text]

    chunks: list[str] = []
    remaining = text.strip()
    while remaining:
        chunk, remaining = _take_chunk(remaining, max_length)
        if chunk:
            chunks.append(chunk)
        remaining = remaining.lstrip()
    return chunks or [""]


def _take_chunk(text: str, max_length: int) -> tuple[str, str]:
    if count_graphemes(text) <= max_length:
        return text.strip(), ""

    sliced = slice_graphemes(text, max_length).rstrip()
    split_at = _best_split_index(sliced, max_length)
    if split_at <= 0:
        split_at = len(sliced)
    return text[:split_at].rstrip(), text[split_at:].lstrip()


def _best_split_index(text: str, max_length: int) -> int:
    paragraph_break = text.rfind("\n\n")
    if paragraph_break >= max(80, max_length // 2):
        return paragraph_break

    sentence_end = None
    for match in _SENTENCE_END_RE.finditer(text):
        sentence_end = match.start() + 1
    if sentence_end and sentence_end >= max(80, max_length // 2):
        return sentence_end

    word_end = text.rfind(" ")
    if word_end >= max(20, max_length // 2):
        return word_end
    return len(text)


def _append_sources_when_possible(
    parts: list[str],
    sources: list[BlueskySource],
    max_length: int,
) -> list[str]:
    source_block = _source_block(sources)
    if not source_block:
        return parts

    updated = list(parts)
    candidate = f"{updated[-1].rstrip()}\n\n{source_block}"
    if count_graphemes(candidate) <= max_length:
        updated[-1] = candidate
    return updated


def _source_block(sources: list[BlueskySource]) -> str:
    if not sources:
        return ""
    lines = ["Sources:"]
    for source in sources:
        label = source.label or "Source"
        lines.append(f"- {label}: {source.url}")
    return "\n".join(lines)


def _embedded_source_rows(content: dict[str, Any]) -> list[dict[str, Any]]:
    rows = content.get("__sources") or []
    return [dict(row) for row in rows]


def _content_lineage(db: Any, content_id: int) -> list[dict[str, Any]]:
    getter = getattr(db, "get_content_lineage", None)
    if callable(getter):
        return [dict(row) for row in getter(content_id)]
    return []


def _source_attributions(
    content_text: str,
    source_rows: list[dict[str, Any]],
) -> list[BlueskySource]:
    attributions: list[BlueskySource] = []
    seen: set[str] = set()

    for url in _links_in_order(content_text):
        if url not in seen:
            seen.add(url)
            attributions.append(BlueskySource(url=url, label="Original link"))

    for row in source_rows:
        url = str(row.get("source_url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        label = str(row.get("author") or row.get("source_type") or "Source").strip()
        attributions.append(BlueskySource(url=url, label=label))

    return attributions


def _links_in_order(text: str) -> list[str]:
    links: list[str] = []
    for match in _URL_RE.finditer(text):
        link = match.group(0).rstrip(".,;:!?")
        if link and link not in links:
            links.append(link)
    return links


def _normalize_spacing(text: str) -> str:
    paragraphs = []
    for paragraph in re.split(r"\n{2,}", text.strip()):
        normalized = _WHITESPACE_RE.sub(" ", paragraph).strip()
        normalized = re.sub(r"\s+([,.;:!?])", r"\1", normalized)
        if normalized:
            paragraphs.append(normalized)
    return "\n\n".join(paragraphs)
