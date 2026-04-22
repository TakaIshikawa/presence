"""Tests for RSS/Atom feed helpers."""

from email.message import Message
from unittest.mock import patch

from knowledge.rss import discover_feed_candidates


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
