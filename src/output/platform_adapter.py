"""Platform-specific text adaptation helpers."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Protocol


BLUESKY_GRAPHEME_LIMIT = 300
LINKEDIN_GRAPHEME_LIMIT = 3000
LINKEDIN_MAX_HASHTAGS = 5
LINKEDIN_PARAGRAPH_GRAPHEME_LIMIT = 700
ELLIPSIS = "..."

_URL_RE = re.compile(r"https?://[^\s<>()]+")
_WHITESPACE_RE = re.compile(r"\s+")
_SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])(?:[\"')\]]+)?\s+")
_HASHTAG_RE = re.compile(r"(?<!\w)#[A-Za-z][A-Za-z0-9_]*")
_THREAD_MARKER_RE = re.compile(
    r"(?im)^\s*(?:(?:tweet|post)\s*\d+|thread)\s*[:.)-]\s*|^\s*\d+\s*[/.)-]\s*"
)
_TWEET_MARKER_LINE_RE = re.compile(r"^TWEET \d+:\s*$", re.IGNORECASE)

_X_PHRASE_REPLACEMENTS = (
    (re.compile(r"\bTwitter/X\b", re.IGNORECASE), "Bluesky"),
    (re.compile(r"\bX/Twitter\b", re.IGNORECASE), "Bluesky"),
    (re.compile(r"\bTwitter\b", re.IGNORECASE), "Bluesky"),
    (re.compile(r"\bX\b"), "Bluesky"),
    (re.compile(r"\btweets\b", re.IGNORECASE), "posts"),
    (re.compile(r"\btweet\b", re.IGNORECASE), "post"),
    (re.compile(r"\btweeted\b", re.IGNORECASE), "posted"),
    (re.compile(r"\btweeting\b", re.IGNORECASE), "posting"),
    (re.compile(r"\bretweets\b", re.IGNORECASE), "reposts"),
    (re.compile(r"\bretweet\b", re.IGNORECASE), "repost"),
    (re.compile(r"\bretweeted\b", re.IGNORECASE), "reposted"),
    (re.compile(r"\bquote tweets\b", re.IGNORECASE), "quote posts"),
    (re.compile(r"\bquote tweet\b", re.IGNORECASE), "quote post"),
)

_X_CLEANUP_PATTERNS = (
    re.compile(r"\b(?:over )?on (?:X|Twitter|X/Twitter|Twitter/X)\b[:,]?\s*", re.IGNORECASE),
    re.compile(r"\b(?:follow|find) me on (?:X|Twitter|X/Twitter|Twitter/X)\b[:,]?\s*", re.IGNORECASE),
    re.compile(r"\b(?:for|to) the (?:X|Twitter) crowd\b[:,]?\s*", re.IGNORECASE),
)


class AdaptationContextProvider(Protocol):
    """Subset of PlatformDivergenceAnalyzer used by the adapter."""

    def generate_adaptation_context(self, days: int = 60) -> str:
        """Return platform adaptation notes."""


@dataclass(frozen=True)
class AdaptationHints:
    """Deterministic preferences derived from platform analysis text."""

    reduce_hashtags: bool = False


def grapheme_clusters(text: str) -> list[str]:
    """Split text into a practical approximation of Unicode grapheme clusters."""
    normalized = unicodedata.normalize("NFC", text)
    clusters: list[str] = []
    regional_run = 0
    join_next = False

    for char in normalized:
        codepoint = ord(char)
        category = unicodedata.category(char)
        is_regional = 0x1F1E6 <= codepoint <= 0x1F1FF
        attaches = (
            category.startswith("M")
            or 0xFE00 <= codepoint <= 0xFE0F
            or 0x1F3FB <= codepoint <= 0x1F3FF
            or char == "\u200d"
            or join_next
            or (is_regional and regional_run % 2 == 1)
        )

        if clusters and attaches:
            clusters[-1] += char
        else:
            clusters.append(char)

        if char == "\u200d":
            join_next = True
        else:
            join_next = False

        regional_run = regional_run + 1 if is_regional else 0

    return clusters


def count_graphemes(text: str) -> int:
    """Count grapheme clusters in text."""
    return len(grapheme_clusters(text))


def slice_graphemes(text: str, limit: int) -> str:
    """Return text truncated to at most limit grapheme clusters."""
    if limit <= 0:
        return ""
    return "".join(grapheme_clusters(text)[:limit])


def variant_type_for_content_type(content_type: str) -> str:
    """Return the durable variant type used for a generated content type."""
    if content_type == "x_thread":
        return "thread"
    return "post"


def split_x_posts(content: str, content_type: str) -> list[str]:
    """Split generated X copy into the platform-neutral post units."""
    if content_type != "x_thread":
        return [content] if content else []

    posts: list[list[str]] = []
    current: list[str] = []
    for line in content.splitlines():
        if _TWEET_MARKER_LINE_RE.match(line):
            if current:
                posts.append(current)
            current = []
        else:
            current.append(line)
    if current:
        posts.append(current)
    return ["\n".join(post).strip() for post in posts if "\n".join(post).strip()]


def format_variant_posts(posts: list[str], content_type: str) -> str:
    """Format post units as durable variant text for later preview/publish parsing."""
    if content_type == "x_thread":
        return "\n\n".join(
            f"TWEET {index}:\n{post}" for index, post in enumerate(posts, start=1)
        )
    return posts[0] if posts else ""


def deterministic_variant_metadata(
    *,
    platform: str,
    content_type: str,
    adapter: str,
    content: str,
    refreshed_at: str | None = None,
) -> dict[str, Any]:
    """Build stable metadata for deterministic platform variants."""
    metadata: dict[str, Any] = {
        "source_content_type": content_type,
        "adapter": adapter,
        "graphemes": count_graphemes(content),
        "deterministic": True,
        "platform": platform,
    }
    if refreshed_at:
        metadata["refreshed_at"] = refreshed_at
    return metadata


def build_bluesky_variant(
    text: str,
    content_type: str,
    *,
    adapter: "BlueskyPlatformAdapter | None" = None,
    suggested_hashtags: list[str] | tuple[str, ...] | None = None,
) -> str:
    """Generate durable Bluesky copy without publishing."""
    platform_adapter = adapter or BlueskyPlatformAdapter()
    posts = split_x_posts(text, content_type)
    adapted = [
        platform_adapter.adapt(
            post,
            content_type,
            suggested_hashtags=(
                suggested_hashtags if index == len(posts) - 1 else None
            ),
        )
        for index, post in enumerate(posts)
    ]
    return format_variant_posts(adapted, content_type)


def build_linkedin_variant(
    text: str,
    content_type: str,
    *,
    adapter: "LinkedInPlatformAdapter | None" = None,
    suggested_hashtags: list[str] | tuple[str, ...] | None = None,
) -> str:
    """Generate durable manual LinkedIn copy without publishing."""
    platform_adapter = adapter or LinkedInPlatformAdapter()
    source = "\n\n".join(split_x_posts(text, content_type))
    return platform_adapter.adapt(
        source,
        content_type=content_type,
        suggested_hashtags=suggested_hashtags,
    )


class BlueskyPlatformAdapter:
    """Deterministically adapt X-oriented text for Bluesky."""

    def __init__(
        self,
        context_provider: AdaptationContextProvider | None = None,
        grapheme_limit: int = BLUESKY_GRAPHEME_LIMIT,
    ):
        self.context_provider = context_provider
        self.grapheme_limit = grapheme_limit

    def adapt(
        self,
        text: str,
        content_type: str = "x_post",
        suggested_hashtags: list[str] | tuple[str, ...] | None = None,
    ) -> str:
        """Return a Bluesky-specific text variant without using an LLM."""
        context = self._adaptation_context()
        hints = self._hints_from_context(context)
        adapted = self._cleanup_x_wording(text)

        if hints.reduce_hashtags:
            adapted = self._reduce_hashtags(adapted)
        else:
            adapted = self._append_hashtags(adapted, suggested_hashtags, max_hashtags=2)

        adapted = self._normalize_spacing(adapted)
        return self._fit_to_limit(adapted)

    def _adaptation_context(self) -> str:
        if not self.context_provider:
            return ""
        generator = getattr(self.context_provider, "generate_adaptation_context", None)
        if not callable(generator):
            return ""
        try:
            return generator()
        except Exception:
            return ""

    def _hints_from_context(self, context: str) -> AdaptationHints:
        lowered = context.lower()
        reduce_hashtags = (
            "bluesky" in lowered
            and ("better on bluesky" in lowered or "more engagement on bluesky" in lowered)
        )
        return AdaptationHints(reduce_hashtags=reduce_hashtags)

    def _cleanup_x_wording(self, text: str) -> str:
        adapted = unicodedata.normalize("NFC", text)

        for pattern in _X_CLEANUP_PATTERNS:
            adapted = pattern.sub("", adapted)

        for pattern, replacement in _X_PHRASE_REPLACEMENTS:
            adapted = pattern.sub(
                lambda match: self._match_case(replacement, match.group(0)),
                adapted,
            )

        return adapted

    def _match_case(self, replacement: str, original: str) -> str:
        if original.isupper() and replacement.lower() != "bluesky":
            return replacement.upper()
        if original[:1].isupper():
            return replacement[:1].upper() + replacement[1:]
        return replacement

    def _reduce_hashtags(self, text: str) -> str:
        matches = list(_HASHTAG_RE.finditer(text))
        if len(matches) <= 2:
            return text

        keep = {match.span() for match in matches[:2]}

        def replace(match: re.Match[str]) -> str:
            return match.group(0) if match.span() in keep else ""

        return _HASHTAG_RE.sub(replace, text)

    def _append_hashtags(
        self,
        text: str,
        suggested_hashtags: list[str] | tuple[str, ...] | None,
        *,
        max_hashtags: int,
    ) -> str:
        if not suggested_hashtags:
            return text

        hashtags = self._merge_hashtags(text, suggested_hashtags, max_hashtags=max_hashtags)
        if not hashtags:
            return text

        cleaned = self._normalize_spacing(_HASHTAG_RE.sub("", text))
        if not cleaned:
            return " ".join(hashtags)
        return f"{cleaned} {' '.join(hashtags)}"

    def _merge_hashtags(
        self,
        text: str,
        suggested_hashtags: list[str] | tuple[str, ...] | None,
        *,
        max_hashtags: int,
    ) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for hashtag in [
            *[match.group(0) for match in _HASHTAG_RE.finditer(text)],
            *(suggested_hashtags or []),
        ]:
            key = hashtag.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(hashtag)
            if len(merged) >= max_hashtags:
                break
        return merged

    def _fit_to_limit(self, text: str) -> str:
        if count_graphemes(text) <= self.grapheme_limit:
            return text

        links = self._links_in_order(text)
        link_suffix = ""
        body = text

        if links:
            preserved_links: list[str] = []
            for link in links:
                candidate = " ".join(preserved_links + [link])
                if count_graphemes(candidate) + count_graphemes(ELLIPSIS) + 1 <= self.grapheme_limit:
                    preserved_links.append(link)
            if preserved_links:
                link_suffix = " " + " ".join(preserved_links)
                body = self._normalize_spacing(_URL_RE.sub("", text))

        suffix = ELLIPSIS + link_suffix
        body_limit = self.grapheme_limit - count_graphemes(suffix)
        if body_limit <= 0:
            return slice_graphemes(suffix, self.grapheme_limit)

        truncated = self._truncate_body_at_sentence(body, body_limit)
        return self._normalize_spacing(truncated + suffix)

    def _truncate_body_at_sentence(self, text: str, grapheme_limit: int) -> str:
        if count_graphemes(text) <= grapheme_limit:
            return text.rstrip()

        sliced = slice_graphemes(text, grapheme_limit).rstrip()
        sentence_end = self._last_sentence_boundary(sliced)
        if sentence_end and count_graphemes(sliced[:sentence_end]) >= 20:
            return sliced[:sentence_end].rstrip()

        word_end = sliced.rfind(" ")
        if word_end >= max(20, grapheme_limit // 2):
            return sliced[:word_end].rstrip()

        return sliced.rstrip()

    def _last_sentence_boundary(self, text: str) -> int | None:
        last_end = None
        for match in _SENTENCE_BOUNDARY_RE.finditer(text):
            last_end = match.start()
        if last_end is None and text.endswith((".", "!", "?")):
            last_end = len(text)
        return last_end

    def _links_in_order(self, text: str) -> list[str]:
        links: list[str] = []
        for match in _URL_RE.finditer(text):
            link = match.group(0).rstrip(".,;:!?")
            if link and link not in links:
                links.append(link)
        return links

    def _normalize_spacing(self, text: str) -> str:
        text = _WHITESPACE_RE.sub(" ", text).strip()
        text = re.sub(r"\s+([,.;:!?])", r"\1", text)
        return text


class LinkedInPlatformAdapter:
    """Deterministically adapt generated text for LinkedIn text posts."""

    def __init__(self, grapheme_limit: int = LINKEDIN_GRAPHEME_LIMIT):
        self.grapheme_limit = grapheme_limit

    def adapt(
        self,
        text: str,
        content_type: str = "x_post",
        suggested_hashtags: list[str] | tuple[str, ...] | None = None,
    ) -> str:
        """Return a LinkedIn-specific text variant without publishing it."""
        adapted = self._cleanup_x_wording(text)
        paragraphs = self._paragraphs_from_text(adapted, content_type)
        body, hashtags = self._extract_hashtags(paragraphs)
        body = self._shape_paragraphs(body)
        hashtags = self._merge_hashtags(hashtags, suggested_hashtags)

        if hashtags:
            body.append(" ".join(hashtags[:LINKEDIN_MAX_HASHTAGS]))

        adapted = "\n\n".join(paragraph for paragraph in body if paragraph).strip()
        return self._fit_to_limit(adapted)

    def _cleanup_x_wording(self, text: str) -> str:
        adapted = unicodedata.normalize("NFC", text)

        for pattern in _X_CLEANUP_PATTERNS:
            adapted = pattern.sub("", adapted)

        replacements = (
            (re.compile(r"\bTwitter/X\b", re.IGNORECASE), "LinkedIn"),
            (re.compile(r"\bX/Twitter\b", re.IGNORECASE), "LinkedIn"),
            (re.compile(r"\bTwitter\b", re.IGNORECASE), "LinkedIn"),
            (re.compile(r"\bX\b"), "LinkedIn"),
            (re.compile(r"\btweets\b", re.IGNORECASE), "posts"),
            (re.compile(r"\btweet\b", re.IGNORECASE), "post"),
            (re.compile(r"\btweeted\b", re.IGNORECASE), "posted"),
            (re.compile(r"\btweeting\b", re.IGNORECASE), "posting"),
            (re.compile(r"\bretweets\b", re.IGNORECASE), "shares"),
            (re.compile(r"\bretweet\b", re.IGNORECASE), "share"),
            (re.compile(r"\bretweeted\b", re.IGNORECASE), "shared"),
            (re.compile(r"\bquote tweets\b", re.IGNORECASE), "quoted posts"),
            (re.compile(r"\bquote tweet\b", re.IGNORECASE), "quoted post"),
        )
        for pattern, replacement in replacements:
            adapted = pattern.sub(
                lambda match: self._match_case(replacement, match.group(0)),
                adapted,
            )

        return adapted

    def _match_case(self, replacement: str, original: str) -> str:
        if original.isupper() and replacement.lower() != "linkedin":
            return replacement.upper()
        if original[:1].isupper():
            return replacement[:1].upper() + replacement[1:]
        return replacement

    def _paragraphs_from_text(self, text: str, content_type: str) -> list[str]:
        text = _THREAD_MARKER_RE.sub("", text)
        raw_paragraphs = [line.strip() for line in re.split(r"\n{1,}", text) if line.strip()]
        if not raw_paragraphs:
            return []

        if content_type == "x_thread":
            return [self._normalize_inline_spacing(paragraph) for paragraph in raw_paragraphs]

        joined = " ".join(self._normalize_inline_spacing(paragraph) for paragraph in raw_paragraphs)
        return [joined]

    def _extract_hashtags(self, paragraphs: list[str]) -> tuple[list[str], list[str]]:
        hashtags: list[str] = []
        seen: set[str] = set()
        body: list[str] = []

        for paragraph in paragraphs:
            for match in _HASHTAG_RE.finditer(paragraph):
                key = match.group(0).lower()
                if key not in seen:
                    seen.add(key)
                    hashtags.append(match.group(0))
            cleaned = self._normalize_inline_spacing(_HASHTAG_RE.sub("", paragraph))
            if cleaned:
                body.append(cleaned)

        return body, hashtags

    def _merge_hashtags(
        self,
        existing: list[str],
        suggested_hashtags: list[str] | tuple[str, ...] | None,
    ) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for hashtag in [*existing, *(suggested_hashtags or [])]:
            key = hashtag.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(hashtag)
            if len(merged) >= LINKEDIN_MAX_HASHTAGS:
                break
        return merged

    def _shape_paragraphs(self, paragraphs: list[str]) -> list[str]:
        shaped: list[str] = []
        for paragraph in paragraphs:
            shaped.extend(self._split_long_paragraph(paragraph))
        return shaped

    def _split_long_paragraph(self, paragraph: str) -> list[str]:
        if count_graphemes(paragraph) <= LINKEDIN_PARAGRAPH_GRAPHEME_LIMIT:
            return [paragraph]

        remaining = paragraph
        parts: list[str] = []
        while count_graphemes(remaining) > LINKEDIN_PARAGRAPH_GRAPHEME_LIMIT:
            sliced = slice_graphemes(remaining, LINKEDIN_PARAGRAPH_GRAPHEME_LIMIT).rstrip()
            split_at = self._last_sentence_boundary(sliced)
            if not split_at or count_graphemes(sliced[:split_at]) < 80:
                split_at = sliced.rfind(" ")
            if split_at < 80:
                split_at = len(sliced)
            parts.append(sliced[:split_at].rstrip())
            remaining = remaining[split_at:].lstrip()

        if remaining:
            parts.append(remaining)
        return parts

    def _fit_to_limit(self, text: str) -> str:
        if count_graphemes(text) <= self.grapheme_limit:
            return text

        links = self._links_in_order(text)
        link_suffix = ""
        body = text

        if links:
            preserved_links: list[str] = []
            for link in links:
                candidate = " ".join(preserved_links + [link])
                if count_graphemes(candidate) + count_graphemes(ELLIPSIS) + 2 <= self.grapheme_limit:
                    preserved_links.append(link)
            if preserved_links:
                link_suffix = "\n\n" + " ".join(preserved_links)
                body = self._normalize_paragraph_spacing(_URL_RE.sub("", text))

        suffix = ELLIPSIS + link_suffix
        body_limit = self.grapheme_limit - count_graphemes(suffix)
        if body_limit <= 0:
            return slice_graphemes(suffix, self.grapheme_limit)

        truncated = slice_graphemes(body, body_limit).rstrip()
        paragraph_break = truncated.rfind("\n\n")
        if paragraph_break >= max(80, body_limit // 2):
            truncated = truncated[:paragraph_break].rstrip()
        else:
            word_end = truncated.rfind(" ")
            if word_end >= max(80, body_limit // 2):
                truncated = truncated[:word_end].rstrip()

        return self._normalize_paragraph_spacing(truncated + suffix)

    def _last_sentence_boundary(self, text: str) -> int | None:
        last_end = None
        for match in _SENTENCE_BOUNDARY_RE.finditer(text):
            last_end = match.start()
        if last_end is None and text.endswith((".", "!", "?")):
            last_end = len(text)
        return last_end

    def _links_in_order(self, text: str) -> list[str]:
        links: list[str] = []
        for match in _URL_RE.finditer(text):
            link = match.group(0).rstrip(".,;:!?")
            if link and link not in links:
                links.append(link)
        return links

    def _normalize_inline_spacing(self, text: str) -> str:
        text = _WHITESPACE_RE.sub(" ", text).strip()
        text = re.sub(r"\s+([,.;:!?])", r"\1", text)
        return text

    def _normalize_paragraph_spacing(self, text: str) -> str:
        paragraphs = [
            self._normalize_inline_spacing(paragraph)
            for paragraph in re.split(r"\n{2,}", text.strip())
            if paragraph.strip()
        ]
        return "\n\n".join(paragraphs)
