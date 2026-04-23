"""Build manual LinkedIn publishing artifacts from generated content."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .platform_adapter import (
    LINKEDIN_GRAPHEME_LIMIT,
    LinkedInPlatformAdapter,
    count_graphemes,
    slice_graphemes,
)
from .x_client import parse_thread_content


_URL_RE = re.compile(r"https?://[^\s<>()]+")
_WHITESPACE_RE = re.compile(r"[ \t]+")
_TERSE_REPLACEMENTS = (
    (re.compile(r"(?i)(?<!\w)w/o(?!\w)"), "without"),
    (re.compile(r"(?i)(?<!\w)w/(?!\w)"), "with"),
    (re.compile(r"(?i)(?<!\w)b/c(?!\w)"), "because"),
    (re.compile(r"(?i)(?<!\w)bc(?!\w)"), "because"),
    (re.compile(r"(?i)(?<!\w)imo(?!\w)"), "in my view"),
    (re.compile(r"(?i)(?<!\w)tbh(?!\w)"), "to be honest"),
    (re.compile(r"(?i)(?<!\w)rn(?!\w)"), "right now"),
    (re.compile(r"(?i)(?<!\w)vs\.?(?!\w)"), "versus"),
    (re.compile(r"(?i)\bdevs\b"), "developers"),
    (re.compile(r"(?i)\binfra\b"), "infrastructure"),
    (re.compile(r"(?i)\bprod\b"), "production"),
    (re.compile(r"(?i)\bdocs\b"), "documentation"),
)


@dataclass(frozen=True)
class LinkedInExportOptions:
    """Configuration for LinkedIn artifact generation."""

    max_length: int = LINKEDIN_GRAPHEME_LIMIT
    include_sources: bool = True


@dataclass(frozen=True)
class SourceAttribution:
    """A link that should stay attached to a manual LinkedIn draft."""

    url: str
    label: str | None = None


@dataclass(frozen=True)
class LinkedInExport:
    """A LinkedIn-ready post plus operator-facing artifact metadata."""

    content_id: int
    content_type: str
    text: str
    sources: tuple[SourceAttribution, ...]
    max_length: int = LINKEDIN_GRAPHEME_LIMIT
    was_trimmed: bool = False
    queue: dict[str, Any] | None = None
    queue_id: int | None = None

    @property
    def graphemes(self) -> int:
        return count_graphemes(self.text)


class LinkedInExportError(ValueError):
    """Raised when a LinkedIn export cannot be built."""


def build_linkedin_export(
    content: dict[str, Any],
    *,
    sources: list[dict[str, Any]] | None = None,
    queue: dict[str, Any] | None = None,
    options: LinkedInExportOptions | None = None,
) -> LinkedInExport:
    """Transform one generated content row into a LinkedIn-ready manual draft."""
    options = options or LinkedInExportOptions()
    if options.max_length <= 0:
        raise LinkedInExportError("max_length must be positive")

    content_id = int(content["id"])
    content_type = content.get("content_type") or "x_post"
    source_text = _source_text_for_linkedin(content.get("content") or "", content_type)
    expanded = expand_terse_x_language(source_text)

    adapter = LinkedInPlatformAdapter(grapheme_limit=options.max_length)
    body = adapter.adapt(expanded, content_type=content_type)
    attributions = _source_attributions(content.get("content") or "", sources or [])

    text, was_trimmed = _compose_post(
        body,
        attributions if options.include_sources else [],
        options.max_length,
    )
    queue_id = queue.get("queue_id") if queue else None
    if queue_id is None and queue:
        queue_id = queue.get("id")

    return LinkedInExport(
        content_id=content_id,
        content_type=content_type,
        text=text,
        sources=tuple(attributions),
        queue=dict(queue) if queue else None,
        max_length=options.max_length,
        was_trimmed=was_trimmed,
        queue_id=queue_id,
    )


def build_linkedin_export_from_db(
    db: Any,
    *,
    content_id: int | None = None,
    queue_id: int | None = None,
    options: LinkedInExportOptions | None = None,
) -> LinkedInExport:
    """Fetch generated or queued content and build its LinkedIn artifact data."""
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
            raise LinkedInExportError(f"publish_queue id {queue_id} not found")
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
            raise LinkedInExportError(f"generated_content id {content_id} not found")
        content = dict(row)

    sources = _content_lineage(db, int(content_id))
    return build_linkedin_export(
        content,
        sources=sources,
        queue=queue,
        options=options,
    )


def write_linkedin_markdown(export: LinkedInExport, path: str | Path) -> Path:
    """Write a markdown artifact for manual LinkedIn publishing."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(format_linkedin_markdown(export), encoding="utf-8")
    return target


def format_linkedin_markdown(export: LinkedInExport) -> str:
    """Render an operator-facing LinkedIn markdown artifact."""
    lines = [
        "# LinkedIn Draft",
        "",
        f"- Content ID: {export.content_id}",
        f"- Content type: {export.content_type}",
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
    lines.extend(
        [
            f"- Length: {export.graphemes}/{export.max_length} graphemes",
            f"- Trimmed: {'yes' if export.was_trimmed else 'no'}",
            "",
            "## Post",
            "",
            export.text,
        ]
    )
    if export.sources:
        lines.extend(["", "## Sources", ""])
        for source in export.sources:
            label = source.label or "Source"
            lines.append(f"- {label}: {source.url}")
    return "\n".join(lines).rstrip() + "\n"


def linkedin_export_to_dict(export: LinkedInExport) -> dict[str, Any]:
    """Return a JSON-safe dictionary for a LinkedIn export."""
    data = asdict(export)
    data["graphemes"] = export.graphemes
    return data


def linkedin_export_to_json(export: LinkedInExport) -> str:
    """Serialize a LinkedIn export as stable JSON."""
    return json.dumps(linkedin_export_to_dict(export), indent=2, sort_keys=True)


def expand_terse_x_language(text: str) -> str:
    """Expand common terse social-post shorthand before LinkedIn formatting."""
    expanded = text
    for pattern, replacement in _TERSE_REPLACEMENTS:
        expanded = pattern.sub(
            lambda match: _match_case(replacement, match.group(0)),
            expanded,
        )
    return _normalize_spacing(expanded)


def _match_case(replacement: str, original: str) -> str:
    if original.isupper():
        return replacement.upper()
    if original[:1].isupper():
        return replacement[:1].upper() + replacement[1:]
    return replacement


def _source_text_for_linkedin(text: str, content_type: str) -> str:
    if content_type == "x_thread":
        posts = parse_thread_content(text)
        return "\n\n".join(posts)
    return text


def _content_lineage(db: Any, content_id: int) -> list[dict[str, Any]]:
    getter = getattr(db, "get_content_lineage", None)
    if callable(getter):
        return [dict(row) for row in getter(content_id)]
    return []


def _source_attributions(
    content_text: str,
    source_rows: list[dict[str, Any]],
) -> list[SourceAttribution]:
    attributions: list[SourceAttribution] = []
    seen: set[str] = set()

    for url in _links_in_order(content_text):
        if url not in seen:
            seen.add(url)
            attributions.append(SourceAttribution(url=url, label="Original link"))

    for row in source_rows:
        url = str(row.get("source_url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        label = str(row.get("author") or row.get("source_type") or "Source").strip()
        attributions.append(SourceAttribution(url=url, label=label))

    return attributions


def _compose_post(
    body: str,
    sources: list[SourceAttribution],
    max_length: int,
) -> tuple[str, bool]:
    source_block = _source_block(sources, max_length)
    candidate = _join_body_and_sources(body, source_block)
    if count_graphemes(candidate) <= max_length:
        return candidate, False

    suffix = "\n\n" + source_block if source_block else ""
    marker = "..."
    body_limit = max_length - count_graphemes(suffix) - count_graphemes(marker)
    if body_limit <= 0:
        return slice_graphemes((source_block or body), max_length), True

    trimmed_body = _trim_body(body, body_limit)
    return _join_body_and_sources(trimmed_body + marker, source_block), True


def _source_block(sources: list[SourceAttribution], max_length: int) -> str:
    if not sources:
        return ""

    lines = ["Sources:"]
    for source in sources:
        label = source.label or "Source"
        candidate_lines = lines + [f"- {label}: {source.url}"]
        candidate = "\n".join(candidate_lines)
        if count_graphemes(candidate) <= max(max_length // 2, 120):
            lines = candidate_lines
    return "\n".join(lines) if len(lines) > 1 else ""


def _join_body_and_sources(body: str, source_block: str) -> str:
    body = body.strip()
    source_block = source_block.strip()
    if body and source_block:
        return f"{body}\n\n{source_block}"
    return body or source_block


def _trim_body(text: str, grapheme_limit: int) -> str:
    sliced = slice_graphemes(text, grapheme_limit).rstrip()
    paragraph_break = sliced.rfind("\n\n")
    if paragraph_break >= max(80, grapheme_limit // 2):
        return sliced[:paragraph_break].rstrip()

    sentence_end = max(sliced.rfind(". "), sliced.rfind("! "), sliced.rfind("? "))
    if sentence_end >= max(80, grapheme_limit // 2):
        return sliced[: sentence_end + 1].rstrip()

    word_end = sliced.rfind(" ")
    if word_end >= max(20, grapheme_limit // 2):
        return sliced[:word_end].rstrip()

    return sliced


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
