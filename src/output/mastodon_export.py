"""Build manual Mastodon publishing artifacts from generated content."""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from synthesis.alt_text_guard import validate_alt_text

from .platform_adapter import count_graphemes
from .platform_adapter import slice_graphemes
from .x_client import parse_thread_content


MASTODON_CHARACTER_LIMIT = 500
_WHITESPACE_RE = re.compile(r"[ \t]+")
_SENTENCE_END_RE = re.compile(r"(?<=[.!?])(?:[\"')\]]+)?\s+")


@dataclass(frozen=True)
class MastodonExportOptions:
    """Configuration for Mastodon artifact generation."""

    limit: int = MASTODON_CHARACTER_LIMIT
    cw: str | None = None
    require_alt_text: bool = True


@dataclass(frozen=True)
class MastodonMedia:
    """Media attachment metadata needed for manual Mastodon publishing."""

    path: str
    alt_text: str
    prompt: str | None = None


@dataclass(frozen=True)
class MastodonStatus:
    """One Mastodon status in a post or thread."""

    index: int
    total: int
    text: str
    cw: str | None = None

    @property
    def characters(self) -> int:
        return count_graphemes(self.text)


@dataclass(frozen=True)
class MastodonExport:
    """Mastodon-ready statuses plus operator-facing artifact metadata."""

    content_id: int
    content_type: str
    statuses: tuple[MastodonStatus, ...]
    media: tuple[MastodonMedia, ...] = ()
    limit: int = MASTODON_CHARACTER_LIMIT
    cw: str | None = None

    @property
    def status_count(self) -> int:
        return len(self.statuses)


class MastodonExportError(ValueError):
    """Raised when a Mastodon export cannot be built."""


def build_mastodon_export(
    content: dict[str, Any],
    *,
    options: MastodonExportOptions | None = None,
) -> MastodonExport:
    """Transform one generated content row into a Mastodon-ready artifact."""
    options = options or MastodonExportOptions()
    if options.limit <= 0:
        raise MastodonExportError("limit must be positive")

    content_id = int(content["id"])
    content_type = content.get("content_type") or "x_post"
    text = _source_text_for_mastodon(content.get("content") or "", content_type)
    media = _media_for_content(content, require_alt_text=options.require_alt_text)
    status_texts = _split_statuses(_normalize_spacing(text), options.limit)

    return MastodonExport(
        content_id=content_id,
        content_type=content_type,
        statuses=tuple(
            MastodonStatus(
                index=index,
                total=len(status_texts),
                text=status,
                cw=options.cw,
            )
            for index, status in enumerate(status_texts, start=1)
        ),
        media=tuple(media),
        limit=options.limit,
        cw=options.cw,
    )


def build_mastodon_export_from_db(
    db: Any,
    *,
    content_id: int,
    options: MastodonExportOptions | None = None,
) -> MastodonExport:
    """Fetch generated content and build Mastodon artifact data."""
    row = db.conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    if not row:
        raise MastodonExportError(f"generated_content id {content_id} not found")
    return build_mastodon_export(dict(row), options=options)


def mastodon_artifact_filename(content_id: int, *, artifact_format: str = "markdown") -> str:
    """Return a stable filename for a Mastodon export artifact."""
    if artifact_format not in {"json", "markdown"}:
        raise ValueError("artifact_format must be 'json' or 'markdown'")
    extension = "json" if artifact_format == "json" else "md"
    return f"mastodon-{content_id}.{extension}"


def write_mastodon_artifact(
    export: MastodonExport,
    output_dir: str | Path,
    *,
    artifact_format: str = "markdown",
) -> Path:
    """Write a Mastodon artifact to an output directory."""
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / mastodon_artifact_filename(
        export.content_id,
        artifact_format=artifact_format,
    )
    if artifact_format == "json":
        target.write_text(mastodon_export_to_json(export) + "\n", encoding="utf-8")
    elif artifact_format == "markdown":
        target.write_text(format_mastodon_markdown(export), encoding="utf-8")
    else:
        raise ValueError("artifact_format must be 'json' or 'markdown'")
    return target


def format_mastodon_markdown(export: MastodonExport) -> str:
    """Render an operator-facing Mastodon markdown artifact."""
    lines = [
        "# Mastodon Draft",
        "",
        f"- Content ID: {export.content_id}",
        f"- Content type: {export.content_type}",
        f"- Statuses: {export.status_count}",
        f"- Limit: {export.limit} characters",
    ]
    if export.cw:
        lines.append(f"- Content warning: {export.cw}")

    if export.media:
        lines.extend(["", "## Media", ""])
        for item in export.media:
            lines.append(f"- Path: {item.path}")
            lines.append(f"  Alt text: {item.alt_text}")
            if item.prompt:
                lines.append(f"  Prompt: {item.prompt}")

    lines.extend(["", "## Statuses", ""])
    for status in export.statuses:
        if export.status_count > 1:
            lines.append(f"### Status {status.index}/{status.total}")
            lines.append("")
        if status.cw:
            lines.append(f"CW: {status.cw}")
            lines.append("")
        lines.append(status.text)
        lines.append("")
        lines.append(f"_Length: {status.characters}/{export.limit} characters_")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def mastodon_export_to_dict(export: MastodonExport) -> dict[str, Any]:
    """Return a JSON-safe dictionary for a Mastodon export."""
    data = asdict(export)
    data["status_count"] = export.status_count
    for status in data["statuses"]:
        status["characters"] = count_graphemes(status["text"])
    return data


def mastodon_export_to_json(export: MastodonExport) -> str:
    """Serialize a Mastodon export as stable JSON."""
    return json.dumps(mastodon_export_to_dict(export), indent=2, sort_keys=True)


def _source_text_for_mastodon(text: str, content_type: str) -> str:
    if content_type == "x_thread":
        return "\n\n".join(parse_thread_content(text))
    return text


def _media_for_content(
    content: dict[str, Any],
    *,
    require_alt_text: bool,
) -> list[MastodonMedia]:
    image_path = str(content.get("image_path") or "").strip()
    if not image_path:
        return []

    alt_text = str(content.get("image_alt_text") or "").strip()
    validation = validate_alt_text(
        alt_text,
        image_prompt=content.get("image_prompt"),
        image_path=image_path,
        content_type=content.get("content_type"),
    )
    if require_alt_text and not validation.passed:
        issues = ", ".join(issue.code for issue in validation.issues)
        raise MastodonExportError(f"media alt text failed validation: {issues}")

    return [
        MastodonMedia(
            path=image_path,
            alt_text=alt_text,
            prompt=content.get("image_prompt"),
        )
    ]


def _split_statuses(text: str, limit: int) -> list[str]:
    if count_graphemes(text) <= limit:
        return [text]

    total = 2
    while True:
        prefix_limit = _numbering_prefix_limit(total)
        body_limit = limit - prefix_limit
        if body_limit <= 0:
            raise MastodonExportError("limit is too small for numbered statuses")
        chunks = _split_body(text, body_limit)
        if len(chunks) == total:
            return [
                _with_numbering(chunk, index, total, limit)
                for index, chunk in enumerate(chunks, start=1)
            ]
        total = len(chunks)


def _split_body(text: str, limit: int) -> list[str]:
    chunks: list[str] = []
    remaining = text.strip()
    while remaining:
        chunk, remaining = _take_chunk(remaining, limit)
        if chunk:
            chunks.append(chunk)
        remaining = remaining.lstrip()
    return chunks or [""]


def _take_chunk(text: str, limit: int) -> tuple[str, str]:
    if count_graphemes(text) <= limit:
        return text.strip(), ""

    sliced = slice_graphemes(text, limit).rstrip()
    split_at = _best_split_index(sliced, limit)
    if split_at <= 0:
        split_at = len(sliced)
    return text[:split_at].rstrip(), text[split_at:].lstrip()


def _best_split_index(text: str, limit: int) -> int:
    paragraph_break = text.rfind("\n\n")
    if paragraph_break >= max(80, limit // 2):
        return paragraph_break

    sentence_end = None
    for match in _SENTENCE_END_RE.finditer(text):
        sentence_end = match.start() + 1
    if sentence_end and sentence_end >= max(80, limit // 2):
        return sentence_end

    word_end = text.rfind(" ")
    if word_end >= max(20, limit // 2):
        return word_end
    return len(text)


def _numbering_prefix_limit(total: int) -> int:
    return max(
        count_graphemes(f"{index}/{total} ")
        for index in range(1, total + 1)
    )


def _with_numbering(text: str, index: int, total: int, limit: int) -> str:
    status = f"{index}/{total} {text.strip()}"
    if count_graphemes(status) > limit:
        raise MastodonExportError("numbered status exceeds Mastodon limit")
    return status


def _normalize_spacing(text: str) -> str:
    paragraphs = []
    for paragraph in re.split(r"\n{2,}", text.strip()):
        normalized = _WHITESPACE_RE.sub(" ", paragraph).strip()
        normalized = re.sub(r"\s+([,.;:!?])", r"\1", normalized)
        if normalized:
            paragraphs.append(normalized)
    return "\n\n".join(paragraphs)
