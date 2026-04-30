"""Build reusable Open Graph and Twitter-card preview metadata."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Iterable

from .blog_metadata import extract_description, extract_title_and_body


DEFAULT_CONTENT_TYPES = ("blog_post", "x_long_post", "x_visual")
_WHITESPACE_RE = re.compile(r"\s+")
_URL_RE = re.compile(r"https?://[^\s<>()]+")


@dataclass(frozen=True)
class PreviewWarning:
    """Explicit metadata warning for downstream card renderers."""

    code: str
    field: str
    message: str


@dataclass(frozen=True)
class SocialPreviewCard:
    """Normalized social preview metadata for one generated_content row."""

    content_id: int
    content_type: str
    title: str
    description: str
    url: str | None
    canonical_url: str | None
    image: str | None
    image_alt_text: str | None
    warnings: list[PreviewWarning]
    open_graph: dict[str, Any]
    twitter_card: dict[str, Any]
    platforms: dict[str, dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SocialPreviewCardError(ValueError):
    """Raised when social preview card input is invalid."""


def build_social_preview_card(row: dict[str, Any]) -> SocialPreviewCard:
    """Build normalized preview metadata from one generated_content row."""
    data = dict(row)
    content_id = int(data["id"])
    content_type = str(data.get("content_type") or "").strip()
    content = str(data.get("content") or "")

    title, description = _title_and_description(content, content_type, content_id)
    url = _text_or_none(data.get("published_url"))
    image = _text_or_none(data.get("image_path"))
    image_alt_text = _text_or_none(data.get("image_alt_text"))
    warnings = _warnings(url=url, image=image, image_alt_text=image_alt_text)
    og_type = _open_graph_type(content_type)

    open_graph = {
        "og:title": title,
        "og:description": description,
        "og:type": og_type,
        "og:url": url,
        "og:image": image,
        "og:image:alt": image_alt_text,
    }
    twitter_card = {
        "twitter:card": "summary_large_image" if image else "summary",
        "twitter:title": title,
        "twitter:description": description,
        "twitter:url": url,
        "twitter:image": image,
        "twitter:image:alt": image_alt_text,
    }

    return SocialPreviewCard(
        content_id=content_id,
        content_type=content_type,
        title=title,
        description=description,
        url=url,
        canonical_url=url,
        image=image,
        image_alt_text=image_alt_text,
        warnings=warnings,
        open_graph=open_graph,
        twitter_card=twitter_card,
        platforms={
            "open_graph": open_graph,
            "twitter": twitter_card,
        },
    )


def build_social_preview_cards(rows: Iterable[dict[str, Any]]) -> list[SocialPreviewCard]:
    """Build preview metadata records from generated_content rows."""
    return [build_social_preview_card(row) for row in rows]


def social_preview_cards_to_json(cards: SocialPreviewCard | list[SocialPreviewCard]) -> str:
    """Serialize preview cards as stable JSON."""
    if isinstance(cards, SocialPreviewCard):
        payload: Any = cards.to_dict()
    else:
        payload = [card.to_dict() for card in cards]
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def social_preview_cards_to_jsonl(cards: Iterable[SocialPreviewCard]) -> str:
    """Serialize preview cards as stable JSON Lines."""
    return "\n".join(
        json.dumps(card.to_dict(), ensure_ascii=False, sort_keys=True) for card in cards
    )


def _title_and_description(
    content: str,
    content_type: str,
    content_id: int,
) -> tuple[str, str]:
    if content_type == "blog_post":
        title, body = extract_title_and_body(content, content_id)
        description = extract_description(body)
        return _shorten(title, 90), _shorten(description, 200)

    title = _content_summary(content, max_length=90) or _fallback_title(content_type, content_id)
    description = _content_summary(content, max_length=200, skip_first=True)
    if not description or description == title:
        description = _content_summary(content, max_length=200) or title
    return title, description


def _content_summary(
    content: str,
    *,
    max_length: int,
    skip_first: bool = False,
) -> str:
    cleaned = _URL_RE.sub("", content)
    cleaned = re.sub(r"^\s*(?:TWEET|POST)\s+\d+\s*[:.-]\s*", "", cleaned, flags=re.I | re.M)
    parts = [
        _normalize_text(part.strip(" #*-`>"))
        for part in re.split(r"(?<=[.!?])\s+|\n+", cleaned)
    ]
    parts = [part for part in parts if part]
    if skip_first and len(parts) > 1:
        parts = parts[1:]
    if not parts:
        return ""
    return _shorten(parts[0], max_length)


def _fallback_title(content_type: str, content_id: int) -> str:
    label = (content_type or "content").replace("_", " ").title()
    return f"{label} {content_id}"


def _shorten(value: str, max_length: int) -> str:
    normalized = _normalize_text(value)
    if len(normalized) <= max_length:
        return normalized
    return normalized[: max_length - 3].rstrip() + "..."


def _normalize_text(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", value).strip()


def _text_or_none(value: Any) -> str | None:
    text = _normalize_text(str(value or ""))
    return text or None


def _open_graph_type(content_type: str) -> str:
    if content_type in {"blog_post", "x_long_post"}:
        return "article"
    return "website"


def _warnings(
    *,
    url: str | None,
    image: str | None,
    image_alt_text: str | None,
) -> list[PreviewWarning]:
    warnings: list[PreviewWarning] = []
    if not url:
        warnings.append(
            PreviewWarning(
                code="missing_url",
                field="published_url",
                message="No canonical URL is available for this preview card.",
            )
        )
    if not image:
        warnings.append(
            PreviewWarning(
                code="missing_image",
                field="image_path",
                message="No preview image is available for this card.",
            )
        )
    if not image_alt_text:
        warnings.append(
            PreviewWarning(
                code="missing_image_alt_text",
                field="image_alt_text",
                message="No image alt text is available for this preview card.",
            )
        )
    return warnings
