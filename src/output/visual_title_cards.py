"""Build deterministic title-card metadata for visual post renderers."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_MAX_TITLE_CHARS = 64
DEFAULT_MAX_SUBTITLE_CHARS = 120
_WHITESPACE_RE = re.compile(r"\s+")
_URL_RE = re.compile(r"https?://[^\s<>()]+")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")

_ACCENTS = (
    "#2563eb",
    "#0f766e",
    "#b45309",
    "#be123c",
    "#6d28d9",
    "#047857",
)


@dataclass(frozen=True)
class SafeArea:
    """Renderer hints for text placement inside a platform card."""

    top: float
    right: float
    bottom: float
    left: float
    unit: str = "percent"


@dataclass(frozen=True)
class PlatformTitleCard:
    """Platform-specific title-card layout metadata."""

    aspect_ratio: str
    canvas: dict[str, int]
    safe_area: SafeArea
    title_anchor: str
    subtitle_anchor: str


@dataclass(frozen=True)
class VisualTitleCard:
    """Reviewable title-card metadata for one generated visual post."""

    content_id: int | None
    content_type: str
    title: str
    subtitle: str
    accent_color: str
    image_style: str | None
    source: str
    platforms: dict[str, PlatformTitleCard]


class VisualTitleCardError(ValueError):
    """Raised when title-card metadata cannot be built."""


def build_visual_title_card(
    preview: dict[str, Any],
    *,
    planned_topic: dict[str, Any] | None = None,
    max_title_chars: int = DEFAULT_MAX_TITLE_CHARS,
    max_subtitle_chars: int = DEFAULT_MAX_SUBTITLE_CHARS,
) -> VisualTitleCard:
    """Build title-card metadata from a publication preview and topic context."""
    if max_title_chars <= 0:
        raise VisualTitleCardError("max_title_chars must be positive")
    if max_subtitle_chars <= 0:
        raise VisualTitleCardError("max_subtitle_chars must be positive")

    content = preview.get("content") or {}
    content_type = content.get("content_type") or "x_visual"
    if content_type != "x_visual":
        raise VisualTitleCardError("title cards can only be exported for x_visual content")

    source_text = _source_text(preview)
    if not source_text:
        raise VisualTitleCardError("visual preview does not include post text")

    title_source = _planned_topic_title(planned_topic) or _content_summary(source_text)
    subtitle_source = (
        _planned_topic_subtitle(planned_topic)
        or _image_prompt_summary(content.get("image_prompt"))
        or _content_summary(source_text, skip_first=True)
        or source_text
    )
    image_style = _image_style(content.get("image_prompt"))

    return VisualTitleCard(
        content_id=_optional_int(content.get("id")),
        content_type=content_type,
        title=_shorten(title_source, max_title_chars),
        subtitle=_shorten(subtitle_source, max_subtitle_chars),
        accent_color=_accent_color(title_source, image_style),
        image_style=image_style,
        source="publication_preview",
        platforms=_platform_hints(),
    )


def build_visual_title_card_from_artifact(
    artifact: dict[str, Any],
    *,
    max_title_chars: int = DEFAULT_MAX_TITLE_CHARS,
    max_subtitle_chars: int = DEFAULT_MAX_SUBTITLE_CHARS,
) -> VisualTitleCard:
    """Build title-card metadata from a visual post dry-run review artifact."""
    preview = artifact.get("preview") or {}
    planned_topic = (artifact.get("run") or {}).get("planned_topic")
    return build_visual_title_card(
        preview,
        planned_topic=planned_topic,
        max_title_chars=max_title_chars,
        max_subtitle_chars=max_subtitle_chars,
    )


def build_visual_title_card_from_db(
    db: Any,
    *,
    content_id: int,
    max_title_chars: int = DEFAULT_MAX_TITLE_CHARS,
    max_subtitle_chars: int = DEFAULT_MAX_SUBTITLE_CHARS,
) -> VisualTitleCard:
    """Fetch one generated visual post and build title-card metadata."""
    from .preview import build_publication_preview

    preview = build_publication_preview(db, content_id=content_id)
    return build_visual_title_card(
        preview,
        planned_topic=fetch_planned_topic_for_content(db, content_id),
        max_title_chars=max_title_chars,
        max_subtitle_chars=max_subtitle_chars,
    )


def build_recent_visual_title_cards_from_db(
    db: Any,
    *,
    limit: int = 5,
    max_title_chars: int = DEFAULT_MAX_TITLE_CHARS,
    max_subtitle_chars: int = DEFAULT_MAX_SUBTITLE_CHARS,
) -> list[VisualTitleCard]:
    """Build title-card metadata for recent generated visual posts."""
    if limit <= 0:
        raise VisualTitleCardError("limit must be positive")

    rows = db.conn.execute(
        """SELECT id
           FROM generated_content
           WHERE content_type = 'x_visual'
           ORDER BY datetime(created_at) DESC, id DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [
        build_visual_title_card_from_db(
            db,
            content_id=int(row["id"]),
            max_title_chars=max_title_chars,
            max_subtitle_chars=max_subtitle_chars,
        )
        for row in rows
    ]


def fetch_planned_topic_for_content(db: Any, content_id: int) -> dict | None:
    """Return planned topic context linked to a generated content id."""
    row = db.conn.execute(
        """SELECT *
           FROM planned_topics
           WHERE content_id = ?
           ORDER BY id DESC
           LIMIT 1""",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


def visual_title_card_to_dict(card: VisualTitleCard) -> dict[str, Any]:
    """Return a JSON-safe dict for one card."""
    return asdict(card)


def visual_title_cards_to_json(cards: VisualTitleCard | list[VisualTitleCard]) -> str:
    """Serialize one or more title-card artifacts as stable JSON."""
    if isinstance(cards, VisualTitleCard):
        payload: Any = visual_title_card_to_dict(cards)
    else:
        payload = [visual_title_card_to_dict(card) for card in cards]
    return json.dumps(payload, indent=2, sort_keys=True)


def write_visual_title_card_artifact(
    card: VisualTitleCard,
    path: str | Path,
) -> Path:
    """Write one title-card artifact as JSON."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(visual_title_cards_to_json(card) + "\n", encoding="utf-8")
    return target


def visual_title_card_filename(content_id: int | None) -> str:
    """Return a stable filename for a visual title-card artifact."""
    suffix = str(content_id) if content_id is not None else "preview"
    return f"visual-title-card-{suffix}.json"


def _source_text(preview: dict[str, Any]) -> str:
    platforms = preview.get("platforms") or {}
    x_posts = (platforms.get("x") or {}).get("posts") or []
    for post in x_posts:
        text = _clean_text(post.get("text") or "")
        if text:
            return text
    bluesky_posts = (platforms.get("bluesky") or {}).get("posts") or []
    for post in bluesky_posts:
        text = _clean_text(post.get("text") or "")
        if text:
            return text
    return ""


def _planned_topic_title(planned_topic: dict[str, Any] | None) -> str:
    if not planned_topic:
        return ""
    return _clean_text(planned_topic.get("topic") or "")


def _planned_topic_subtitle(planned_topic: dict[str, Any] | None) -> str:
    if not planned_topic:
        return ""
    return _clean_text(planned_topic.get("angle") or "")


def _content_summary(text: str, *, skip_first: bool = False) -> str:
    parts = [
        _clean_text(part).strip(" .")
        for part in _SENTENCE_RE.split(_clean_text(text))
        if _clean_text(part)
    ]
    if skip_first and len(parts) > 1:
        return parts[1]
    return parts[0] if parts else _clean_text(text)


def _image_prompt_summary(image_prompt: Any) -> str:
    parts = [part.strip() for part in str(image_prompt or "").split("|")]
    if len(parts) >= 3:
        return _clean_text(parts[2])
    if len(parts) == 2:
        return _clean_text(parts[1])
    return ""


def _image_style(image_prompt: Any) -> str | None:
    style = str(image_prompt or "").split("|", 1)[0].strip().lower()
    return style or None


def _platform_hints() -> dict[str, PlatformTitleCard]:
    return {
        "x": PlatformTitleCard(
            aspect_ratio="16:9",
            canvas={"width": 1600, "height": 900},
            safe_area=SafeArea(top=10, right=8, bottom=12, left=8),
            title_anchor="left_center",
            subtitle_anchor="left_below_title",
        ),
        "bluesky": PlatformTitleCard(
            aspect_ratio="1.91:1",
            canvas={"width": 1200, "height": 628},
            safe_area=SafeArea(top=12, right=9, bottom=13, left=9),
            title_anchor="left_center",
            subtitle_anchor="left_below_title",
        ),
    }


def _accent_color(seed: str, image_style: str | None) -> str:
    key = f"{image_style or ''}:{seed}"
    return _ACCENTS[sum(ord(char) for char in key) % len(_ACCENTS)]


def _shorten(text: str, max_chars: int) -> str:
    text = _clean_text(text).strip('"')
    if len(text) <= max_chars:
        return text.rstrip(".")
    return text[: max_chars - 3].rstrip(" ,;:.") + "..."


def _clean_text(text: Any) -> str:
    text = _URL_RE.sub("", str(text or ""))
    return _WHITESPACE_RE.sub(" ", text).strip()


def _optional_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
