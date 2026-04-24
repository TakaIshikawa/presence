"""Tests for RSS/Atom feed helpers."""

from email.message import Message
from unittest.mock import patch
from pathlib import Path

from knowledge.rss import discover_feed_candidates
from knowledge.link_metadata import parse_link_metadata


class _MockPageResponse:
    def __init__(self, body: str, headers: dict[str, str] | None = None):
        self._body = body.encode("utf-8")
        self.headers = Message()
        for key, value in (headers or {}).items():
            self.headers[key] = value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self._body


@patch("knowledge.rss.urlopen")
def test_discovers_ranked_feed_candidates_from_alternate_links(mock_urlopen):
    mock_urlopen.return_value = _MockPageResponse(
        """
        <html>
          <head>
            <link rel="stylesheet" href="/site.css">
            <link rel="alternate" type="application/feed+json" href="/feed.json" title="JSON">
            <link rel="alternate feed" type="application/rss+xml; charset=utf-8" href="/rss.xml">
            <link rel="ALTERNATE" type="application/atom+xml" href="atom.xml" title="Atom">
            <link rel="alternate" type="application/atom+xml" href="atom.xml" title="Duplicate">
          </head>
        </html>
        """
    )

    candidates = discover_feed_candidates("https://example.com/blog/", timeout=3)

    assert [candidate.url for candidate in candidates] == [
        "https://example.com/blog/atom.xml",
        "https://example.com/rss.xml",
        "https://example.com/feed.json",
    ]
    assert candidates[0].content_type == "application/atom+xml"
    assert candidates[0].title == "Atom"
    assert mock_urlopen.call_args.kwargs["timeout"] == 3


def test_parse_link_metadata_normalizes_canonical_and_image_urls():
    fixture = Path(__file__).parent / "fixtures" / "article_metadata.html"

    metadata = parse_link_metadata(fixture.read_text(), "https://example.com/posts/source")

    assert metadata.canonical_url == "https://example.com/canonical/article?utm_source=rss"
    assert metadata.title == "Canonical Article Title"
    assert metadata.description == "A compact summary of the article."
    assert metadata.site_name == "Example Journal"
    assert metadata.image == "https://example.com/images/article-card.png"


def test_parse_link_metadata_handles_missing_metadata():
    metadata = parse_link_metadata("<html><head><title>Only Title</title></head>", "https://example.com/a")

    assert metadata.to_dict() == {"title": "Only Title"}
