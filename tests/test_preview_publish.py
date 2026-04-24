"""Tests for publication preview rendering."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.preview import (  # noqa: E402
    PreviewRecordNotFound,
    build_publication_preview,
    format_preview,
    preview_to_json,
)


def _insert_content(
    db,
    content="Hello from X",
    content_type="x_post",
    image_path=None,
    image_prompt=None,
    image_alt_text=None,
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
        image_alt_text=image_alt_text,
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
    assert preview["platforms"]["bluesky"]["posts"][0]["source"] == "fresh_adapted"
    assert preview["platforms"]["bluesky"]["status"]["status"] == "generated"
    assert preview["platforms"]["x"]["status"]["requested"] is True
    assert preview["platforms"]["bluesky"]["status"]["requested"] is True


def test_preview_prefers_stored_platform_variant(db):
    content_id = _insert_content(db, "Tweeting this on X")
    variant_id = db.upsert_content_variant(
        content_id,
        "bluesky",
        "post",
        "Stored Bluesky review copy",
        {"source": "manual"},
    )

    preview = build_publication_preview(db, content_id=content_id)

    post = preview["platforms"]["bluesky"]["posts"][0]
    assert post["text"] == "Stored Bluesky review copy"
    assert post["source"] == "stored_variant"
    assert post["variant_id"] == variant_id


def test_preview_does_not_overwrite_stored_variant_without_refresh(db):
    content_id = _insert_content(db, "Tweeting this on X")
    db.upsert_content_variant(
        content_id,
        "bluesky",
        "post",
        "Manual reviewed copy",
        {"source": "manual"},
    )

    preview = build_publication_preview(db, content_id=content_id)

    variant = db.get_content_variant(content_id, "bluesky", "post")
    assert preview["platforms"]["bluesky"]["posts"][0]["text"] == "Manual reviewed copy"
    assert variant["content"] == "Manual reviewed copy"
    assert variant["metadata"] == {"source": "manual"}


def test_preview_refresh_variants_replaces_deterministic_variants(db):
    content_id = _insert_content(db, "Tweeting this on X")
    original_id = db.upsert_content_variant(
        content_id,
        "bluesky",
        "post",
        "Stale deterministic copy",
        {"source": "old"},
    )

    preview = build_publication_preview(db, content_id=content_id, refresh_variants=True)

    variant = db.get_content_variant(content_id, "bluesky", "post")
    linkedin = db.get_content_variant(content_id, "linkedin", "post")
    assert variant["id"] == original_id
    assert variant["content"] == "Posting this"
    assert variant["metadata"]["adapter"] == "BlueskyPlatformAdapter"
    assert variant["metadata"]["deterministic"] is True
    assert "refreshed_at" in variant["metadata"]
    assert linkedin["metadata"]["adapter"] == "LinkedInPlatformAdapter"
    assert preview["platforms"]["bluesky"]["posts"][0]["source"] == "stored_variant"
    assert {item["platform"] for item in preview["refreshed_variants"]} == {
        "bluesky",
        "linkedin",
    }


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
    assert preview["platforms"]["x"]["thread_preflight"]["status"] == "passed"
    assert preview["platforms"]["bluesky"]["thread_preflight"]["status"] == "passed"


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


def test_preview_publish_cli_refreshes_variants(db, capsys):
    content_id = _insert_content(db, "Tweeting from X")

    import preview_publish

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("preview_publish.script_context", return_value=Context()):
        exit_code = preview_publish.main(
            ["--content-id", str(content_id), "--json", "--refresh-variants"]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["platforms"]["bluesky"]["posts"][0]["source"] == "stored_variant"
    assert db.get_content_variant(content_id, "bluesky", "post")["content"] == "Posting from Bluesky"
    assert db.get_content_variant(content_id, "linkedin", "post") is not None


def test_preview_publish_cli_blocks_failed_alt_text_in_strict_mode(db, capsys):
    content_id = _insert_content(
        db,
        "CLI visual preview",
        content_type="x_visual",
        image_path="/tmp/presence-images/visual.png",
        image_prompt="Launch metrics dashboard",
    )

    import preview_publish

    class Context:
        def __enter__(self):
            config = SimpleNamespace(
                publishing=SimpleNamespace(alt_text_guard_mode="strict")
            )
            return config, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("preview_publish.script_context", return_value=Context()):
        exit_code = preview_publish.main(["--content-id", str(content_id)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "Alt text guard failed:" in captured.err
    assert "missing_alt_text" in captured.err


def test_preview_publish_cli_warns_for_failed_alt_text_in_warning_mode(db, capsys):
    content_id = _insert_content(
        db,
        "CLI visual preview",
        content_type="x_visual",
        image_path="/tmp/presence-images/visual.png",
        image_prompt="Launch metrics dashboard",
    )

    import preview_publish

    class Context:
        def __enter__(self):
            config = SimpleNamespace(
                publishing=SimpleNamespace(alt_text_guard_mode="warning")
            )
            return config, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("preview_publish.script_context", return_value=Context()):
        exit_code = preview_publish.main(["--content-id", str(content_id)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Alt text guard warning:" in captured.err
    assert "Alt text guard: failed" in captured.out


def test_preview_publish_cli_writes_linkedin_artifact(db, tmp_path, capsys):
    content_id = _insert_content(db, "CLI artifact post w/ source")
    queue_id = db.queue_for_publishing(
        content_id,
        "2026-04-17T12:00:00+00:00",
        platform="all",
    )
    artifact_path = tmp_path / "linkedin.md"

    import preview_publish

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("preview_publish.script_context", return_value=Context()):
        exit_code = preview_publish.main(
            [
                "--queue-id",
                str(queue_id),
                "--linkedin-out",
                str(artifact_path),
                "--linkedin-max-length",
                "500",
            ]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"LinkedIn artifact: {artifact_path}" in captured.err
    artifact = artifact_path.read_text()
    assert artifact.startswith("# LinkedIn Draft")
    assert f"- Content ID: {content_id}" in artifact
    assert f"- Queue ID: {queue_id}" in artifact
    assert "CLI artifact post with source" in artifact


def test_preview_publish_cli_shows_hashtag_suggestions(db, capsys):
    content_id = _insert_content(
        db,
        "Python API testing workflow shipped today.",
    )
    db.insert_content_topics(content_id, [("testing", "api tests", 0.9)])

    import preview_publish

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("preview_publish.script_context", return_value=Context()):
        exit_code = preview_publish.main(
            ["--content-id", str(content_id), "--suggest-hashtags"]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Suggested hashtags:" in captured.out
    assert "- X: #Testing #API #Python" in captured.out
    assert "- BLUESKY: #Testing #API" in captured.out
    assert "- LINKEDIN: #Testing #API #Python #Workflow" in captured.out
    assert "Python API testing workflow shipped today. #Testing #API" in captured.out


def test_preview_json_includes_hashtag_suggestions_when_requested(db):
    content_id = _insert_content(db, "Debugging Python latency in production.")
    db.insert_content_topics(content_id, [("debugging", "latency", 0.8)])

    preview = build_publication_preview(
        db,
        content_id=content_id,
        include_hashtag_suggestions=True,
    )

    payload = json.loads(preview_to_json(preview))
    assert payload["hashtag_suggestions"]["x"] == [
        "#Debugging",
        "#Performance",
        "#Python",
    ]
    assert payload["platforms"]["bluesky"]["suggested_hashtags"] == [
        "#Debugging",
        "#Performance",
    ]


def test_preview_publish_cli_blocks_restricted_knowledge_in_strict_mode(db, capsys):
    content_id = _insert_content(db, "Restricted-source preview")
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "restricted-preview",
            "https://source.example/restricted",
            "Source Author",
            "Restricted source context",
            "restricted",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    import preview_publish

    class Context:
        def __enter__(self):
            config = SimpleNamespace(
                curated_sources=SimpleNamespace(restricted_prompt_behavior="strict")
            )
            return config, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("preview_publish.script_context", return_value=Context()):
        exit_code = preview_publish.main(["--content-id", str(content_id)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "License guard blocked:" in captured.err
    assert f"knowledge {knowledge_id}: restricted https://source.example/restricted" in captured.err


def test_preview_publish_cli_allows_restricted_knowledge_with_override(db, capsys):
    content_id = _insert_content(db, "Restricted-source preview")
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "restricted-preview-override",
            "https://source.example/restricted",
            "Source Author",
            "Restricted source context",
            "restricted",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    import preview_publish

    class Context:
        def __enter__(self):
            config = SimpleNamespace(
                curated_sources=SimpleNamespace(restricted_prompt_behavior="strict")
            )
            return config, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("preview_publish.script_context", return_value=Context()):
        exit_code = preview_publish.main(
            [
                "--content-id",
                str(content_id),
                "--allow-restricted-knowledge",
            ]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "License guard warning:" in captured.err
    assert "License guard: warning (1 restricted sources)" in captured.out


def test_preview_publish_cli_blocks_missing_attribution(db, capsys):
    content_id = _insert_content(db, "Attribution-required preview")
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "attribution-preview",
            "https://source.example/attribution",
            "Source Author",
            "Attribution-required source context",
            "attribution_required",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    import preview_publish

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("preview_publish.script_context", return_value=Context()):
        exit_code = preview_publish.main(["--content-id", str(content_id)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "Attribution guard blocked:" in captured.err
    assert (
        f"knowledge {knowledge_id}: attribution_required "
        "Source Author https://source.example/attribution"
    ) in captured.err


def test_preview_includes_claim_check_summary_in_text_and_json(db):
    content_id = _insert_content(db, "Post with 43% unsupported claim")
    db.save_claim_check_summary(
        content_id,
        supported_count=1,
        unsupported_count=1,
        annotation_text="metric: Post with 43% unsupported claim (metric value not found in sources)",
    )

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["claim_check"]["status"] == "unsupported_claims"
    assert preview["claim_check"]["supported_count"] == 1
    assert preview["claim_check"]["unsupported_count"] == 1
    assert "metric: Post with 43%" in preview["claim_check"]["annotation_text"]

    text = format_preview(preview)
    assert "Claim check: unsupported_claims (1 supported, 1 unsupported)" in text
    assert "Unsupported claims:" in text
    assert "- metric: Post with 43% unsupported claim" in text

    payload = json.loads(preview_to_json(preview))
    assert payload["claim_check"]["status"] == "unsupported_claims"
    assert payload["claim_check"]["unsupported_count"] == 1


def test_preview_queue_id_includes_claim_check_summary(db):
    content_id = _insert_content(db, "Queued post")
    queue_id = db.queue_for_publishing(
        content_id,
        "2026-04-17T12:00:00+00:00",
        platform="x",
    )
    db.save_claim_check_summary(
        content_id,
        supported_count=2,
        unsupported_count=0,
        annotation_text=None,
    )

    preview = build_publication_preview(db, queue_id=queue_id)

    assert preview["queue"]["queue_id"] == queue_id
    assert preview["claim_check"]["checked"] is True
    assert preview["claim_check"]["status"] == "supported"
    assert "Claim check: supported (2 supported, 0 unsupported)" in format_preview(preview)


def test_preview_includes_failed_thread_preflight_status(db):
    content_id = _insert_content(
        db,
        "TWEET 1:\nFirst\nTWEET 2:\n\n",
        content_type="x_thread",
    )
    queue_id = db.queue_for_publishing(
        content_id,
        "2026-04-17T12:00:00+00:00",
        platform="all",
    )

    preview = build_publication_preview(db, queue_id=queue_id)

    x_preflight = preview["platforms"]["x"]["thread_preflight"]
    bsky_preflight = preview["platforms"]["bluesky"]["thread_preflight"]
    assert x_preflight["status"] == "failed"
    assert x_preflight["issues"][0]["code"] == "empty_post"
    assert bsky_preflight["status"] == "failed"

    text = format_preview(preview)
    assert "Thread preflight: failed (2 posts)" in text
    assert "- post 2: empty_post: Thread post text is empty" in text


def test_preview_includes_persona_guard_summary(db):
    content_id = _insert_content(db, "Generic post")
    db.save_persona_guard_summary(
        content_id,
        {
            "checked": True,
            "passed": False,
            "status": "failed",
            "score": 0.31,
            "reasons": ["banned tone markers: unlock"],
            "metrics": {"banned_marker_count": 1},
        },
    )

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["persona_guard"]["checked"] is True
    assert preview["persona_guard"]["passed"] is False
    assert preview["persona_guard"]["status"] == "failed"
    assert preview["persona_guard"]["score"] == pytest.approx(0.31)
    assert preview["persona_guard"]["reasons"] == ["banned tone markers: unlock"]

    text = format_preview(preview)
    assert "Persona guard: failed (score 0.31)" in text
    assert "- banned tone markers: unlock" in text
