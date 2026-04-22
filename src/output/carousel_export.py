"""Convert generated X threads into carousel planning outlines."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .x_client import parse_thread_content


DEFAULT_MAX_SLIDES = 8
DEFAULT_MAX_BULLETS = 3
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_URL_RE = re.compile(r"https?://[^\s<>()]+")
_NUMBER_PREFIX_RE = re.compile(r"^\s*\d+\s*/\s*\d*\s*")
_WHITESPACE_RE = re.compile(r"\s+")
_METRIC_RE = re.compile(r"(?<!\w)(\d+(?:\.\d+)?\s*(?:%|x|ms|s|min|hrs?|days?|k|m)?)(?!\w)", re.I)
_COMPARISON_RE = re.compile(
    r"\b(before|after|instead|versus|vs\.?|but|from|to|without|with)\b",
    re.I,
)


@dataclass(frozen=True)
class CarouselSlide:
    """One slide outline for later visual design."""

    index: int
    title: str
    body_bullets: list[str]
    visual_note: str
    alt_text_prompt: str
    source_post: str
    visual_prompt_convention: str


@dataclass(frozen=True)
class CarouselExport:
    """Design artifact generated from a thread."""

    content_id: int | None
    content_type: str
    slide_count: int
    max_slides: int
    slides: list[CarouselSlide]
    source: str = "x_thread"


class CarouselExportError(ValueError):
    """Raised when a carousel export cannot be built."""


def build_carousel_export(
    content: dict[str, Any],
    *,
    max_slides: int = DEFAULT_MAX_SLIDES,
    max_bullets: int = DEFAULT_MAX_BULLETS,
) -> CarouselExport:
    """Build a carousel outline from a generated content row-like dict."""
    if max_slides <= 0:
        raise CarouselExportError("max_slides must be positive")
    if max_bullets <= 0:
        raise CarouselExportError("max_bullets must be positive")

    content_type = content.get("content_type") or "x_thread"
    posts = _posts_from_content(content.get("content") or "", content_type)
    slides = [
        _build_slide(index, post, max_bullets=max_bullets)
        for index, post in enumerate(posts[:max_slides], start=1)
    ]

    return CarouselExport(
        content_id=_optional_int(content.get("id")),
        content_type=content_type,
        slide_count=len(slides),
        max_slides=max_slides,
        slides=slides,
        source="generated_content",
    )


def build_carousel_export_from_preview(
    preview: dict[str, Any],
    *,
    max_slides: int = DEFAULT_MAX_SLIDES,
    max_bullets: int = DEFAULT_MAX_BULLETS,
) -> CarouselExport:
    """Build a carousel outline from publication preview JSON."""
    content = preview.get("content") or {}
    x_posts = (
        (preview.get("platforms") or {})
        .get("x", {})
        .get("posts", [])
    )
    posts = [str(post.get("text") or "").strip() for post in x_posts]
    posts = [post for post in posts if post]

    if max_slides <= 0:
        raise CarouselExportError("max_slides must be positive")
    if max_bullets <= 0:
        raise CarouselExportError("max_bullets must be positive")

    slides = [
        _build_slide(index, post, max_bullets=max_bullets)
        for index, post in enumerate(posts[:max_slides], start=1)
    ]
    return CarouselExport(
        content_id=_optional_int(content.get("id")),
        content_type=content.get("content_type") or "x_thread",
        slide_count=len(slides),
        max_slides=max_slides,
        slides=slides,
        source="publication_preview",
    )


def build_carousel_export_from_db(
    db: Any,
    *,
    content_id: int,
    max_slides: int = DEFAULT_MAX_SLIDES,
    max_bullets: int = DEFAULT_MAX_BULLETS,
) -> CarouselExport:
    """Fetch generated content by id and build a carousel outline."""
    row = db.conn.execute(
        "SELECT * FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()
    if not row:
        raise CarouselExportError(f"generated_content id {content_id} not found")
    return build_carousel_export(
        dict(row),
        max_slides=max_slides,
        max_bullets=max_bullets,
    )


def carousel_to_dict(export: CarouselExport) -> dict[str, Any]:
    """Return a JSON-safe dict for an export."""
    return asdict(export)


def carousel_to_json(export: CarouselExport) -> str:
    """Serialize a carousel artifact as stable JSON."""
    return json.dumps(carousel_to_dict(export), indent=2, sort_keys=True)


def format_carousel_markdown(export: CarouselExport) -> str:
    """Render a design-facing markdown artifact."""
    lines = [
        "# Carousel Slide Outline",
        "",
        f"- Content ID: {export.content_id if export.content_id is not None else 'preview'}",
        f"- Content type: {export.content_type}",
        f"- Slides: {export.slide_count}/{export.max_slides}",
    ]
    for slide in export.slides:
        lines.extend(
            [
                "",
                f"## Slide {slide.index}: {slide.title}",
                "",
                "### Body Bullets",
            ]
        )
        lines.extend(f"- {bullet}" for bullet in slide.body_bullets)
        lines.extend(
            [
                "",
                "### Visual Notes",
                "",
                slide.visual_note,
                "",
                "### Alt Text Prompt",
                "",
                slide.alt_text_prompt,
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def write_carousel_artifact(
    export: CarouselExport,
    path: str | Path,
    *,
    artifact_format: str = "json",
) -> Path:
    """Write a JSON or markdown carousel artifact."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if artifact_format == "json":
        body = carousel_to_json(export) + "\n"
    elif artifact_format == "markdown":
        body = format_carousel_markdown(export)
    else:
        raise CarouselExportError("artifact_format must be 'json' or 'markdown'")
    target.write_text(body, encoding="utf-8")
    return target


def _posts_from_content(content: str, content_type: str) -> list[str]:
    if content_type == "x_thread" or "TWEET" in content:
        posts = parse_thread_content(content)
    else:
        posts = [content.strip()] if content.strip() else []
    return [post for post in posts if post.strip()]


def _build_slide(index: int, post: str, *, max_bullets: int) -> CarouselSlide:
    cleaned = _clean_post(post)
    parts = _split_into_parts(cleaned)
    title = _shorten(parts[0] if parts else f"Slide {index}", 70)
    bullet_candidates = parts[1:] or _fallback_bullets(cleaned, title)
    bullets = [_shorten(part, 120) for part in bullet_candidates[:max_bullets]]
    if not bullets:
        bullets = [_shorten(cleaned, 120)]

    convention = _visual_prompt_convention(title, bullets, cleaned)
    visual_note = _visual_note(convention, title, bullets)
    alt_text_prompt = _alt_text_prompt(index, title, bullets, visual_note)
    return CarouselSlide(
        index=index,
        title=title,
        body_bullets=bullets,
        visual_note=visual_note,
        alt_text_prompt=alt_text_prompt,
        source_post=post.strip(),
        visual_prompt_convention=convention,
    )


def _clean_post(text: str) -> str:
    text = _URL_RE.sub("", text)
    text = _NUMBER_PREFIX_RE.sub("", text.strip())
    return _WHITESPACE_RE.sub(" ", text).strip(" -")


def _split_into_parts(text: str) -> list[str]:
    chunks = []
    for part in _SENTENCE_RE.split(text):
        part = part.strip(" -")
        if part:
            chunks.append(part)
    if len(chunks) <= 1 and ":" in text:
        head, tail = text.split(":", 1)
        chunks = [head.strip(), tail.strip()]
    return [chunk for chunk in chunks if chunk]


def _fallback_bullets(text: str, title: str) -> list[str]:
    if text != title and title in text:
        remainder = text.replace(title, "", 1).strip(" .:-")
        if remainder:
            return [remainder]
    return [text]


def _visual_prompt_convention(title: str, bullets: list[str], text: str) -> str:
    metric = _METRIC_RE.search(text)
    if metric:
        return (
            "METRIC | {label} | {value} | {context}".format(
                label=_shorten(title, 24),
                value=metric.group(1).strip(),
                context=_shorten(" ".join(bullets), 48),
            )
        )
    if _COMPARISON_RE.search(text):
        before = bullets[0] if bullets else "current state"
        after = bullets[1] if len(bullets) > 1 else title
        return (
            "COMPARISON | {title} | {before} | {after}".format(
                title=_shorten(title, 28),
                before=_shorten(before, 42),
                after=_shorten(after, 54),
            )
        )
    return (
        "ANNOTATED | {title} | {body}".format(
            title=_shorten(title, 32),
            body=_shorten(" ".join(bullets), 96),
        )
    )


def _visual_note(convention: str, title: str, bullets: list[str]) -> str:
    visual_type = convention.split("|", 1)[0].strip()
    if visual_type == "METRIC":
        return (
            "Use the visual post METRIC convention: make the number the dominant "
            f"element, label it \"{title}\", and keep supporting text to "
            f"{len(bullets)} short callouts."
        )
    if visual_type == "COMPARISON":
        return (
            "Use the visual post COMPARISON convention: split the slide into two "
            "clear states with the title as the bridge between them."
        )
    return (
        "Use the visual post ANNOTATED convention: turn the title into the main "
        "headline and set the bullets as concise supporting annotations."
    )


def _alt_text_prompt(
    index: int,
    title: str,
    bullets: list[str],
    visual_note: str,
) -> str:
    bullet_text = "; ".join(bullets)
    return (
        f"Write concise alt text for carousel slide {index}: title \"{title}\"; "
        f"body points: {bullet_text}. Describe the planned visual treatment: "
        f"{visual_note} Keep it under 300 characters."
    )


def _shorten(text: str, max_chars: int) -> str:
    text = _WHITESPACE_RE.sub(" ", text.strip().strip('"'))
    if len(text) <= max_chars:
        return text.rstrip(".")
    return text[: max_chars - 3].rstrip(" ,;:.") + "..."


def _optional_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
