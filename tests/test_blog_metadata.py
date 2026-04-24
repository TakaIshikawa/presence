"""Tests for generated blog social metadata export."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from output.blog_metadata import (
    BlogMetadataExporter,
    extract_title_and_body,
    metadata_to_json,
    metadata_to_markdown,
)


def _published_blog(
    db,
    content: str,
    *,
    published_url: str | None = "https://example.com/blog/post.html",
    published_at: str = "2026-04-24T12:00:00+00:00",
    image_path: str | None = "/images/post.png",
    image_alt_text: str | None = "Diagram showing the release flow.",
) -> int:
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
        image_path=image_path,
        image_alt_text=image_alt_text,
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, published_url = ?, published_at = ?
           WHERE id = ?""",
        (published_url, published_at, content_id),
    )
    db.conn.commit()
    return content_id


def test_export_content_id_includes_social_canonical_and_topics(db):
    content_id = _published_blog(
        db,
        "TITLE: Release Notes\n\nThis is the preview description.\n\n## Details",
    )
    db.insert_content_topics(
        content_id,
        [
            ("architecture", "module boundaries", 0.9),
            ("testing", "pytest", 0.8),
        ],
    )

    metadata = BlogMetadataExporter(db).export_content_id(content_id).to_dict()

    assert metadata["content_id"] == content_id
    assert metadata["title"] == "Release Notes"
    assert metadata["description"] == "This is the preview description."
    assert metadata["canonical_url"] == "https://example.com/blog/post.html"
    assert metadata["og_type"] == "article"
    assert metadata["image"] == "/images/post.png"
    assert metadata["image_alt_text"] == "Diagram showing the release flow."
    assert metadata["published_at"] == "2026-04-24T12:00:00+00:00"
    assert metadata["topics"] == ["architecture", "testing"]
    assert metadata["open_graph"]["og:title"] == "Release Notes"
    assert metadata["open_graph"]["article:tag"] == ["architecture", "testing"]
    assert metadata["twitter_card"]["twitter:card"] == "summary_large_image"
    assert metadata["warnings"] == []


def test_markdown_without_explicit_title_gets_deterministic_fallback():
    content = "No title marker here.\n\nA useful first paragraph."

    assert extract_title_and_body(content, content_id=42) == (
        "Blog Post 42",
        content,
    )
    assert extract_title_and_body(content, content_id=42) == (
        "Blog Post 42",
        content,
    )


def test_export_warnings_for_missing_canonical_url_and_image_alt_text(db):
    content_id = _published_blog(
        db,
        "TITLE: Missing Metadata\n\nDescription.",
        published_url=None,
        image_path="/images/missing-alt.png",
        image_alt_text=None,
    )

    metadata = BlogMetadataExporter(db).export_content_id(content_id)

    assert metadata.warnings == ["missing_canonical_url", "missing_image_alt_text"]


def test_export_recent_returns_published_blog_posts_within_window(db):
    now = datetime(2026, 4, 24, 12, tzinfo=timezone.utc)
    recent_id = _published_blog(
        db,
        "TITLE: Recent\n\nRecent description.",
        published_at=(now - timedelta(days=1)).isoformat(),
    )
    _published_blog(
        db,
        "TITLE: Old\n\nOld description.",
        published_at=(now - timedelta(days=10)).isoformat(),
    )
    draft_id = db.insert_generated_content(
        "blog_post",
        [],
        [],
        "TITLE: Draft\n\nDraft description.",
        8.0,
        "ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published_at = ? WHERE id = ?",
        ((now - timedelta(days=1)).isoformat(), draft_id),
    )
    db.conn.commit()

    metadata = BlogMetadataExporter(db).export_recent(days=7, now=now)

    assert [item.content_id for item in metadata] == [recent_id]


def test_export_content_id_rejects_non_blog_post(db):
    content_id = db.insert_generated_content(
        "x_post",
        [],
        [],
        "Short social post.",
        8.0,
        "ok",
    )

    with pytest.raises(ValueError, match="is not a blog_post"):
        BlogMetadataExporter(db).export_content_id(content_id)


def test_metadata_formatters(db):
    content_id = _published_blog(db, "TITLE: Formatters\n\nFormatter description.")
    metadata = BlogMetadataExporter(db).export_content_id(content_id)

    json_payload = json.loads(metadata_to_json(metadata))
    markdown_payload = metadata_to_markdown([metadata])

    assert json_payload["title"] == "Formatters"
    assert "## Formatters" in markdown_payload
    assert '"twitter:card": "summary_large_image"' in markdown_payload
