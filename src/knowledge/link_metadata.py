"""Lightweight HTML link metadata extraction."""

from __future__ import annotations

from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class LinkMetadata:
    """Normalized metadata for a curated article URL."""

    canonical_url: str = ""
    title: str = ""
    description: str = ""
    site_name: str = ""
    image: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            key: value
            for key, value in {
                "canonical_url": self.canonical_url,
                "title": self.title,
                "description": self.description,
                "site_name": self.site_name,
                "image": self.image,
            }.items()
            if value
        }


class LinkMetadataError(RuntimeError):
    """Raised when link metadata cannot be fetched."""


class _LinkMetadataParser(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__()
        self.page_url = page_url
        self.canonical_url = ""
        self.title = ""
        self.description = ""
        self.site_name = ""
        self.image = ""
        self._in_title = False
        self._title_chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {name.lower(): value or "" for name, value in attrs}
        tag = tag.lower()
        if tag == "title":
            self._in_title = True
            return

        rel_tokens = {token.lower() for token in attr_map.get("rel", "").split()}
        if tag == "link" and "canonical" in rel_tokens:
            href = attr_map.get("href", "").strip()
            if href:
                self.canonical_url = urljoin(self.page_url, href)
            return

        if tag != "meta":
            return

        prop = (attr_map.get("property") or attr_map.get("name") or "").lower()
        content = _clean_text(attr_map.get("content", ""))
        if not content:
            return
        if prop == "og:url":
            self.canonical_url = urljoin(self.page_url, content)
        elif prop == "og:title":
            self.title = content
        elif prop in {"description", "og:description"} and not self.description:
            self.description = content
        elif prop == "og:site_name":
            self.site_name = content
        elif prop == "og:image":
            self.image = urljoin(self.page_url, content)

    def handle_data(self, data: str) -> None:
        if self._in_title and data.strip():
            self._title_chunks.append(data.strip())

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False

    def metadata(self) -> LinkMetadata:
        title = self.title or _clean_text(" ".join(self._title_chunks))
        return LinkMetadata(
            canonical_url=self.canonical_url,
            title=title,
            description=self.description,
            site_name=self.site_name,
            image=self.image,
        )


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(unescape(value).split())


def parse_link_metadata(html_text: str, page_url: str) -> LinkMetadata:
    """Extract canonical/title/description/site/image metadata from HTML."""
    parser = _LinkMetadataParser(page_url)
    parser.feed(html_text or "")
    return parser.metadata()


def fetch_link_metadata(url: str, timeout: float = 10.0) -> LinkMetadata:
    """Fetch an article URL and extract lightweight link metadata."""
    headers = {"User-Agent": "PresenceBot/1.0 (+https://github.com/)"}
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            html_text = response.read().decode(charset, errors="replace")
    except URLError as exc:
        raise LinkMetadataError(f"Failed to fetch link metadata for {url}: {exc.reason}") from exc

    return parse_link_metadata(html_text, url)
