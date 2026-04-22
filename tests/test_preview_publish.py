"""Tests for publication preview rendering."""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.preview import (  # noqa: E402
    PreviewRecordNotFound,
    build_publication_preview,
    format_preview,
)


def _insert_content(
    db,
    content="Hello from X",
    content_type="x_post",
    image_path=None,
    image_prompt=None,
):
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
        image_path=image_path,
        image_prompt=image_prompt,
    )


def test_preview_single_post_for_content_id(db):
    content_id = _insert_content(db, "Tweeting this on X with 👩‍💻")

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["content"]["id"] == content_id
    assert preview["queue"] is None
    assert preview["platforms"]["x"]["posts"][0]["text"] == "Tweeting this on X with 👩‍💻"
    assert preview["platforms"]["x"]["posts"][0]["counts"]["characters"] == len(
        "Tweeting this on X with 👩‍💻"
    )
    assert preview["platforms"]["x"]["posts"][0]["counts"]["graphemes"] == 25
    assert preview["platforms"]["bluesky"]["posts"][0]["text"] == "Posting this with 👩‍💻"
    assert preview["platforms"]["bluesky"]["status"]["status"] == "generated"
    assert preview["platforms"]["x"]["status"]["requested"] is True
    assert preview["platforms"]["bluesky"]["status"]["requested"] is True


def test_preview_queue_thread_splits_and_adapts(db):
    content = "TWEET 1:\nFirst tweet for Twitter\nTWEET 2:\nSecond on X"
    content_id = _insert_content(db, content, content_type="x_thread")
    queue_id = db.queue_for_publishing(
        content_id,
        "2026-04-17T12:00:00+00:00",
        platform="all",
    )

    preview = build_publication_preview(db, queue_id=queue_id)

    assert preview["queue"]["queue_id"] == queue_id
    assert [post["text"] for post in preview["platforms"]["x"]["posts"]] == [
        "First tweet for Twitter",
        "Second on X",
    ]
    assert [post["text"] for post in preview["platforms"]["bluesky"]["posts"]] == [
        "First post for Bluesky",
        "Second",
    ]
    assert preview["platforms"]["x"]["posts"][0]["total"] == 2
    assert preview["platforms"]["bluesky"]["status"]["status"] == "queued"


def test_preview_visual_post_includes_image_path_and_publication_status(db):
    content_id = _insert_content(
        db,
        "Visual launch post",
        content_type="x_visual",
        image_path="/tmp/presence-images/visual.png",
        image_prompt="ANNOTATED | Launch | Visual launch post",
    )
    queue_id = db.queue_for_publishing(
        content_id,
        "2026-04-17T12:00:00+00:00",
        platform="x",
    )
    db.mark_published(
        content_id,
        "https://x.com/example/status/123",
        tweet_id="123",
    )

    preview = build_publication_preview(db, queue_id=queue_id)

    assert preview["content"]["image_path"] == "/tmp/presence-images/visual.png"
    assert preview["content"]["image_prompt"] == "ANNOTATED | Launch | Visual launch post"
    assert preview["platforms"]["x"]["image_path"] == "/tmp/presence-images/visual.png"
    assert preview["platforms"]["bluesky"]["image_path"] == "/tmp/presence-images/visual.png"
    assert preview["platforms"]["x"]["status"]["status"] == "published"
    assert preview["platforms"]["x"]["status"]["platform_post_id"] == "123"
    assert preview["platforms"]["x"]["status"]["platform_url"] == (
        "https://x.com/example/status/123"
    )
    assert preview["platforms"]["bluesky"]["status"]["requested"] is False

    text = format_preview(preview)
    assert "Image: /tmp/presence-images/visual.png" in text
    assert "X (requested, status: published)" in text


def test_preview_missing_content_record_raises(db):
    with pytest.raises(PreviewRecordNotFound, match="generated_content id 999 not found"):
        build_publication_preview(db, content_id=999)


def test_preview_missing_queue_record_raises(db):
    with pytest.raises(PreviewRecordNotFound, match="publish_queue id 999 not found"):
        build_publication_preview(db, queue_id=999)


def test_preview_publish_cli_outputs_json(db, capsys):
    content_id = _insert_content(db, "CLI preview post")

    import preview_publish

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("preview_publish.script_context", return_value=Context()):
        exit_code = preview_publish.main(["--content-id", str(content_id), "--json"])

    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["content"]["id"] == content_id
    assert payload["platforms"]["x"]["posts"][0]["text"] == "CLI preview post"
