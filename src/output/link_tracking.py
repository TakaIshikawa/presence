"""Decorate outbound links with campaign tracking parameters."""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


UTM_PARAM_ORDER = ("utm_source", "utm_medium", "utm_campaign")
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
TRAILING_BARE_URL_PUNCTUATION = ".,;:!?"


@dataclass(frozen=True)
class LinkMatch:
    """One link occurrence found in an export artifact."""

    url: str
    start: int
    end: int
    context: str = "text"


@dataclass(frozen=True)
class DecoratedLink:
    """Diagnostics for one link occurrence after decoration was attempted."""

    original_url: str
    decorated_url: str
    changed: bool
    skipped: bool
    reason: str
    start: int
    end: int
    context: str


@dataclass(frozen=True)
class DecorationResult:
    """Transformed content plus deterministic link diagnostics."""

    content: str
    links: list[DecoratedLink]

    @property
    def decorated_count(self) -> int:
        return sum(1 for link in self.links if link.changed)

    @property
    def skipped_count(self) -> int:
        return sum(1 for link in self.links if link.skipped)

    def to_dict(self) -> dict:
        return {
            "content": self.content,
            "decorated_count": self.decorated_count,
            "skipped_count": self.skipped_count,
            "links": [
                {
                    "original_url": link.original_url,
                    "decorated_url": link.decorated_url,
                    "changed": link.changed,
                    "skipped": link.skipped,
                    "reason": link.reason,
                    "start": link.start,
                    "end": link.end,
                    "context": link.context,
                }
                for link in self.links
            ],
        }


MARKDOWN_LINK_RE = re.compile(r"(?<!!)\[[^\]\n]+\]\((?P<url>[^)\s]+)\)")
HTML_LINK_RE = re.compile(
    r"""(?P<attr>\b(?:href|src)\s*=\s*)(?P<quote>["'])(?P<url>.*?)(?P=quote)""",
    re.IGNORECASE,
)
BARE_URL_RE = re.compile(r"https?://[^\s<>'\"\])]+", re.IGNORECASE)


def extract_links(text: str) -> list[LinkMatch]:
    """Return links in deterministic source order.

    Markdown and HTML attribute links are preferred over bare URL detection so
    the same URL span is not reported twice.
    """

    matches: list[LinkMatch] = []
    protected_spans: list[tuple[int, int]] = []

    for match in MARKDOWN_LINK_RE.finditer(text):
        start, end = match.span("url")
        matches.append(LinkMatch(match.group("url"), start, end, "markdown"))
        protected_spans.append((start, end))

    for match in HTML_LINK_RE.finditer(text):
        start, end = match.span("url")
        matches.append(LinkMatch(match.group("url"), start, end, "html"))
        protected_spans.append((start, end))

    for match in BARE_URL_RE.finditer(text):
        start, end = _trim_bare_url_span(text, match.start(), match.end())
        if any(
            start >= protected_start and end <= protected_end
            for protected_start, protected_end in protected_spans
        ):
            continue
        if start == end:
            continue
        matches.append(LinkMatch(text[start:end], start, end, "text"))

    return sorted(matches, key=lambda item: (item.start, item.end))


def decorate_url(
    url: str,
    *,
    utm_source: str | None = None,
    utm_medium: str | None = None,
    utm_campaign: str | None = None,
    replace: bool = False,
) -> tuple[str, bool, str]:
    """Decorate a single URL, preserving query strings and fragments."""

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return url, False, "unsupported_scheme"
    hostname = (parsed.hostname or "").lower()
    if not parsed.netloc or hostname in LOCAL_HOSTS or hostname.endswith(".local"):
        return url, False, "local_link"

    requested_params = {
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign": utm_campaign,
    }
    requested_params = {
        key: str(value)
        for key, value in requested_params.items()
        if value is not None and str(value) != ""
    }
    if not requested_params:
        return url, False, "no_utm_params"

    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    existing_keys = {key for key, _ in query_items}
    changed = False

    if replace:
        query_items = [
            (key, requested_params.get(key, value))
            for key, value in query_items
        ]
        changed = any(key in existing_keys for key in requested_params)
        existing_keys = {key for key, _ in query_items}

    for key in UTM_PARAM_ORDER:
        value = requested_params.get(key)
        if value is None:
            continue
        if key in existing_keys:
            continue
        query_items.append((key, value))
        changed = True

    if not changed:
        return url, False, "utm_exists"

    decorated = urlunparse(parsed._replace(query=urlencode(query_items, doseq=True)))
    return decorated, True, "decorated"


def decorate_links(
    text: str,
    *,
    utm_source: str | None = None,
    utm_medium: str | None = None,
    utm_campaign: str | None = None,
    replace: bool = False,
) -> DecorationResult:
    """Apply tracking parameters to every eligible link in text."""

    matches = extract_links(text)
    replacements: list[tuple[int, int, str]] = []
    diagnostics: list[DecoratedLink] = []

    for match in matches:
        decorated_url, changed, reason = decorate_url(
            match.url,
            utm_source=utm_source,
            utm_medium=utm_medium,
            utm_campaign=utm_campaign,
            replace=replace,
        )
        if changed:
            replacements.append((match.start, match.end, decorated_url))
        diagnostics.append(
            DecoratedLink(
                original_url=match.url,
                decorated_url=decorated_url,
                changed=changed,
                skipped=not changed,
                reason=reason,
                start=match.start,
                end=match.end,
                context=match.context,
            )
        )

    transformed = text
    for start, end, replacement in reversed(replacements):
        transformed = transformed[:start] + replacement + transformed[end:]

    return DecorationResult(content=transformed, links=diagnostics)


def _trim_bare_url_span(text: str, start: int, end: int) -> tuple[int, int]:
    while end > start and text[end - 1] in TRAILING_BARE_URL_PUNCTUATION:
        end -= 1
    return start, end
