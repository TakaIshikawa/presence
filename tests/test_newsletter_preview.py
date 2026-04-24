"""Tests for newsletter draft preview artifact export."""

import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from output.newsletter_preview import (
    assemble_newsletter_preview,
    extract_outbound_links,
    format_preview_markdown,
)

PROJECT_ROOT = Path(__file__).parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "preview_newsletter.py"
spec = importlib.util.spec_from_file_location("preview_newsletter_script", SCRIPT_PATH)
preview_newsletter = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = preview_newsletter
spec.loader.exec_module(preview_newsletter)


def _make_config(enabled=True):
    config = MagicMock()
    config.newsletter.enabled = enabled
    config.newsletter.api_key = ""
    config.newsletter.utm_source = "newsletter"
    config.newsletter.utm_medium = "email"
    config.newsletter.utm_campaign_template = "weekly-{week_end_compact}"
    return config


@contextmanager
def _script_context(config, db):
    yield config, db


def _insert_published_content(
    db,
    content_type,
    content,
    *,
    published_at,
    url=None,
    content_format=None,
):
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha-1"],
        source_messages=["msg-1"],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
        content_format=content_format,
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, published_at = ?, published_url = ?
           WHERE id = ?""",
        (published_at.isoformat(), url or f"https://example.com/{content_id}", content_id),
    )
    db.conn.commit()
    return content_id


def _insert_knowledge(db, *, source_type="rss", source_url=""):
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, author, content, approved, source_url)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (source_type, "source-1", "Analyst", "External source", 1, source_url),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_preview_payload_includes_review_artifact_shape_and_warnings(db):
    week_end = datetime(2026, 4, 24, tzinfo=timezone.utc)
    week_start = week_end - timedelta(days=7)
    blog_id = _insert_published_content(
        db,
        "blog_post",
        "TITLE: Previewable Newsletter\n\nIntro paragraph.\n\nMore detail.",
        published_at=week_end - timedelta(days=1),
        url="https://takaishikawa.com/blog/previewable-newsletter.html",
    )
    post_id = _insert_published_content(
        db,
        "x_post",
        "A note with supporting research.",
        published_at=week_end - timedelta(days=2),
        url="https://x.com/taka/status/123",
        content_format="observation",
    )
    db.insert_engagement(
        content_id=blog_id,
        tweet_id="blog-1",
        like_count=1,
        retweet_count=2,
        reply_count=3,
        quote_count=4,
        engagement_score=10.0,
    )
    knowledge_id = _insert_knowledge(db, source_url="")
    db.insert_content_knowledge_links(post_id, [(knowledge_id, 0.9)])

    payload = assemble_newsletter_preview(
        db,
        _make_config(),
        week_start,
        week_end,
    )

    assert payload["subject"]
    assert payload["intro"].startswith("# Weekly Digest")
    assert [post["id"] for post in payload["selected_posts"]] == [blog_id, post_id]
    assert payload["source_content_ids"] == [blog_id, post_id]
    assert any(section["title"] == "This Week's Post" for section in payload["body_sections"])
    assert "https://takaishikawa.com/blog/previewable-newsletter.html" in {
        link["url"].split("?")[0] for link in payload["outbound_links"]
    }
    assert payload["selected_posts"][0]["metrics"]["has_metrics"] is True
    assert payload["selected_posts"][1]["metrics"]["has_metrics"] is False
    assert payload["warning_metadata"]["by_type"]["missing_metrics"] == 1
    assert payload["warning_metadata"]["by_type"]["missing_citation"] == 1
    assert payload["warnings"][1]["knowledge_id"] == knowledge_id

    rendered = format_preview_markdown(payload)
    assert "# Newsletter Preview" in rendered
    assert "## Selected Posts" in rendered
    assert "## Warnings" in rendered
    assert "missing_metrics" in rendered


def test_extract_outbound_links_deduplicates_markdown_and_bare_urls():
    links = extract_outbound_links(
        "[Read](https://example.com/a) and https://example.com/a plus "
        "[Other](https://example.com/b)."
    )

    assert links == [
        {"url": "https://example.com/a", "label": "Read"},
        {"url": "https://example.com/b", "label": "Other"},
    ]


def test_cli_writes_markdown_and_json_without_buttondown(db, tmp_path):
    week_end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    _insert_published_content(
        db,
        "x_post",
        "CLI preview body.",
        published_at=week_end - timedelta(days=1),
        url="https://x.com/taka/status/456",
    )
    config = _make_config()
    markdown_path = tmp_path / "newsletter-preview.md"
    json_path = tmp_path / "newsletter-preview.json"

    with patch.object(
        preview_newsletter,
        "script_context",
        return_value=_script_context(config, db),
    ):
        result = preview_newsletter.main(
            ["--markdown-out", str(markdown_path), "--json-out", str(json_path)]
        )

    assert result == 0
    assert not hasattr(preview_newsletter, "ButtondownClient")
    assert markdown_path.read_text().startswith("# Newsletter Preview")
    payload = json.loads(json_path.read_text())
    assert payload["body_markdown"]
    assert payload["source_content_ids"]
    assert payload["outbound_links"][0]["url"] == "https://x.com/taka/status/456"


def test_cli_requires_output_path(capsys):
    result = preview_newsletter.main([])

    assert result == 2
    assert "Provide --out" in capsys.readouterr().err
