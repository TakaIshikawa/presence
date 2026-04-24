"""RSS/Atom feed fetching and parsing for curated sources."""

from __future__ import annotations

from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urljoin
import xml.etree.ElementTree as ET


@dataclass(frozen=True)
class FeedEntry:
    """A normalized entry from an RSS or Atom feed."""

    title: str
    link: str
    summary: str
    content: str
    published_at: str | None = None


@dataclass(frozen=True)
class FeedFetchResult:
    """Result of fetching a feed, including HTTP cache validators."""

    entries: list[FeedEntry]
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


@dataclass(frozen=True)
class FeedCandidate:
    """A feed URL discovered from a page's alternate link tags."""

    url: str
    content_type: str
    title: str = ""
    score: int = 0


class FeedFetchError(RuntimeError):
    """Raised when a feed cannot be fetched or parsed."""


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._chunks: list[str] = []

    def handle_data(self, data: str) -> None:
        if data.strip():
            self._chunks.append(data.strip())

    def text(self) -> str:
        return " ".join(self._chunks)


class _FeedLinkExtractor(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__()
        self._page_url = page_url
        self.candidates: list[FeedCandidate] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "link":
            return
        attr_map = {name.lower(): value or "" for name, value in attrs}
        rel_tokens = {token.lower() for token in attr_map.get("rel", "").split()}
        if "alternate" not in rel_tokens:
            return

        href = attr_map.get("href", "").strip()
        content_type = _normalize_media_type(attr_map.get("type", ""))
        if not href or content_type not in _FEED_MEDIA_TYPE_SCORES:
            return

        self.candidates.append(
            FeedCandidate(
                url=urljoin(self._page_url, href),
                content_type=content_type,
                title=_clean_text(attr_map.get("title", "")),
                score=_FEED_MEDIA_TYPE_SCORES[content_type],
            )
        )


_FEED_MEDIA_TYPE_SCORES = {
    "application/atom+xml": 100,
    "application/rss+xml": 90,
    "application/feed+json": 50,
}


def _normalize_media_type(value: str) -> str:
    return value.split(";", 1)[0].strip().lower()


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _children(element: ET.Element, name: str) -> Iterable[ET.Element]:
    return (child for child in element if _local_name(child.tag) == name)


def _first_child_text(element: ET.Element, names: tuple[str, ...]) -> str:
    for child in element:
        if _local_name(child.tag) in names:
            return _clean_text("".join(child.itertext()))
    return ""


def _first_child_raw_text(element: ET.Element, names: tuple[str, ...]) -> str:
    for name in names:
        for child in element:
            if _local_name(child.tag) == name:
                return " ".join("".join(child.itertext()).split())
    return ""


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    parser = _HTMLTextExtractor()
    parser.feed(unescape(value))
    text = parser.text() or unescape(value)
    return " ".join(text.split())


def discover_feed_candidates(page_url: str, timeout: float = 20.0) -> list[FeedCandidate]:
    """Fetch a page and return ranked RSS/Atom/JSON feed candidates."""
    headers = {"User-Agent": "PresenceBot/1.0 (+https://github.com/)"}
    request = Request(page_url, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            html_text = response.read().decode(charset, errors="replace")
    except URLError as exc:
        raise FeedFetchError(f"Failed to fetch page {page_url}: {exc.reason}") from exc

    parser = _FeedLinkExtractor(page_url)
    parser.feed(html_text)
    seen: set[str] = set()
    ranked: list[FeedCandidate] = []
    for candidate in sorted(parser.candidates, key=lambda item: item.score, reverse=True):
        if candidate.url in seen:
            continue
        ranked.append(candidate)
        seen.add(candidate.url)
    return ranked


def _rss_link(item: ET.Element) -> str:
    link = _first_child_text(item, ("link",))
    if link:
        return link
    return _first_child_text(item, ("guid",))


def _atom_link(entry: ET.Element) -> str:
    for child in _children(entry, "link"):
        href = child.attrib.get("href")
        rel = child.attrib.get("rel", "alternate")
        if href and rel == "alternate":
            return href
    for child in _children(entry, "link"):
        href = child.attrib.get("href")
        if href:
            return href
        text = _clean_text("".join(child.itertext()))
        if text:
            return text
    return ""


def parse_feed(xml_text: str, limit: int = 5) -> list[FeedEntry]:
    """Parse RSS or Atom XML into recent feed entries.

    Feeds conventionally order newest entries first, so limiting preserves the
    recent subset without imposing feed-specific date parsing.
    """
    root = ET.fromstring(xml_text)
    root_name = _local_name(root.tag)
    entries: list[FeedEntry] = []

    if root_name == "feed":
        raw_entries = [child for child in root if _local_name(child.tag) == "entry"]
        for entry in raw_entries:
            title = _first_child_text(entry, ("title",))
            link = _atom_link(entry)
            summary = _first_child_text(entry, ("summary",))
            content = _first_child_text(entry, ("content",)) or summary
            published_at = _first_child_raw_text(entry, ("published", "updated"))
            if link and (title or content):
                entries.append(
                    FeedEntry(
                        title=title or link,
                        link=link,
                        summary=summary,
                        content=content,
                        published_at=published_at or None,
                    )
                )
            if len(entries) >= limit:
                break
        return entries

    raw_items = list(root.iter())
    for item in (el for el in raw_items if _local_name(el.tag) == "item"):
        title = _first_child_text(item, ("title",))
        link = _rss_link(item)
        summary = _first_child_text(item, ("description", "summary"))
        content = _first_child_text(item, ("encoded", "content")) or summary
        published_at = _first_child_raw_text(item, ("pubDate", "date", "published", "updated"))
        if link and (title or content):
            entries.append(
                FeedEntry(
                    title=title or link,
                    link=link,
                    summary=summary,
                    content=content,
                    published_at=published_at or None,
                )
            )
        if len(entries) >= limit:
            break

    return entries


def fetch_feed(
    feed_url: str,
    limit: int = 5,
    timeout: float = 20.0,
    etag: str | None = None,
    last_modified: str | None = None,
) -> FeedFetchResult:
    """Fetch and parse a feed URL, using HTTP validators when provided."""
    headers = {"User-Agent": "PresenceBot/1.0 (+https://github.com/)"}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    request = Request(feed_url, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            xml_text = response.read().decode(charset, errors="replace")
            try:
                entries = parse_feed(xml_text, limit=limit)
            except ET.ParseError as exc:
                raise FeedFetchError(f"Failed to parse feed XML from {feed_url}: {exc}") from exc
            return FeedFetchResult(
                entries=entries,
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
            )
    except HTTPError as exc:
        if exc.code != 304:
            raise
        return FeedFetchResult(
            entries=[],
            etag=exc.headers.get("ETag") or etag,
            last_modified=exc.headers.get("Last-Modified") or last_modified,
            not_modified=True,
        )
    except URLError as exc:
        raise FeedFetchError(f"Failed to fetch feed {feed_url}: {exc.reason}") from exc


def fetch_feed_entries(feed_url: str, limit: int = 5, timeout: float = 20.0) -> list[FeedEntry]:
    """Fetch and parse a feed URL."""
    return fetch_feed(feed_url, limit=limit, timeout=timeout).entries
