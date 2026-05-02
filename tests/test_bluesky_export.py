"""Tests for manual Bluesky publishing artifacts."""

import json

import pytest

from output.bluesky_export import (
    BlueskyExportError,
    BlueskyExportOptions,
    bluesky_export_to_json,
    build_bluesky_export,
    build_bluesky_export_from_db,
    count_graphemes,
    format_bluesky_markdown,
)
from scripts.export_bluesky import parse_args


def _insert_content(
    db,
    content: str,
    content_type: str = "x_post",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )
    db.set_curation_quality(content_id, "good")
    return content_id


def test_single_x_post_exports_under_default_bluesky_limit():
    export = build_bluesky_export(
        {
            "id": 7,
            "content_type": "x_post",
            "content": "Short post for manual Bluesky publishing.",
        }
    )

    assert export.post_count == 1
    assert export.posts[0].text == "Short post for manual Bluesky publishing."
    assert export.posts[0].graphemes <= 300
    assert export.posts[0].graphemes <= export.max_length


def test_x_thread_and_overlong_posts_are_split_without_losing_text():
    first = "First thread post."
    long_second = " ".join(f"segment{i}" for i in range(90))
    content = f"TWEET 1:\n{first}\nTWEET 2:\n{long_second}"

    export = build_bluesky_export(
        {
            "id": 8,
            "content_type": "x_thread",
            "content": content,
        },
        options=BlueskyExportOptions(max_length=120),
    )

    assert export.post_count > 2
    assert [post.index for post in export.posts] == list(range(1, export.post_count + 1))
    assert all(post.total == export.post_count for post in export.posts)
    assert all(post.graphemes <= 120 for post in export.posts)
    assert " ".join(post.text for post in export.posts) == f"{first} {long_second}"


def test_source_link_is_preserved_in_post_when_it_fits():
    export = build_bluesky_export(
        {
            "id": 9,
            "content_type": "x_post",
            "content": "Rollout notes: https://example.com/rollout",
        }
    )

    assert export.sources[0].url == "https://example.com/rollout"
    assert "Sources:" in export.posts[0].text
    assert "- Original link: https://example.com/rollout" in export.posts[0].text


def test_queue_export_preserves_queue_metadata_in_markdown_and_json(db):
    content_id = _insert_content(db, "Queue export should keep metadata.")
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, hold_reason)
           VALUES (?, ?, ?, ?, ?)""",
        (
            content_id,
            "2026-04-24T09:00:00+00:00",
            "bluesky",
            "held",
            "manual review",
        ),
    ).lastrowid

    export = build_bluesky_export_from_db(db, queue_id=queue_id)
    markdown = format_bluesky_markdown(export)
    payload = json.loads(bluesky_export_to_json(export))

    assert export.queue_id == queue_id
    assert export.queue is not None
    assert export.queue["scheduled_at"] == "2026-04-24T09:00:00+00:00"
    assert export.queue["platform"] == "bluesky"
    assert "- Queue ID: " in markdown
    assert "- Scheduled at: 2026-04-24T09:00:00+00:00" in markdown
    assert "- Platform: bluesky" in markdown
    assert "- Queue status: held" in markdown
    assert "- Hold reason: manual review" in markdown
    assert payload["queue"]["scheduled_at"] == "2026-04-24T09:00:00+00:00"
    assert payload["queue_id"] == queue_id
    assert payload["posts"][0]["graphemes"] == count_graphemes(export.posts[0].text)


def test_markdown_artifact_contains_ordered_posts_and_source_metadata():
    export = build_bluesky_export(
        {
            "id": 10,
            "content_type": "x_post",
            "content": " ".join(["Manual export"] * 20),
        },
        queue={"queue_id": 3},
        options=BlueskyExportOptions(max_length=80),
    )

    markdown = format_bluesky_markdown(export)

    assert markdown.startswith("# Bluesky Draft")
    assert "- Content ID: 10" in markdown
    assert "- Queue ID: 3" in markdown
    assert "### Post 1/" in markdown
    assert "_Length: " in markdown


def test_db_export_includes_lineage_sources_without_persisting_variant(db):
    content_id = _insert_content(db, "Short sourced post.")
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, approved)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "article-1",
            "https://source.example.com/post",
            "Jane Builder",
            "Useful rollout background",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    export = build_bluesky_export_from_db(db, content_id=content_id)
    variant = db.get_content_variant(content_id, "bluesky", "post")

    assert export.sources[0].url == "https://source.example.com/post"
    assert "Jane Builder" in format_bluesky_markdown(export)
    assert variant is None


def test_invalid_db_selectors_raise_clear_value_error(db):
    with pytest.raises(ValueError, match="Pass exactly one of content_id or queue_id"):
        build_bluesky_export_from_db(db)

    with pytest.raises(ValueError, match="Pass exactly one of content_id or queue_id"):
        build_bluesky_export_from_db(db, content_id=1, queue_id=2)


def test_missing_content_raises_clear_error(db):
    with pytest.raises(BlueskyExportError, match="generated_content id 999 not found"):
        build_bluesky_export_from_db(db, content_id=999)


def test_export_script_accepts_required_options():
    args = parse_args(["--content-id", "1", "--format", "json", "--max-length", "280"])

    assert args.content_id == 1
    assert args.queue_id is None
    assert args.format == "json"
    assert args.max_length == 280
