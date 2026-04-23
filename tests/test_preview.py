"""Tests for publication preview alt-text guard output."""

import json

from output.preview import (
    build_publication_preview,
    format_preview,
    format_visual_post_artifact,
    preview_to_json,
    visual_post_artifact_filename,
    visual_post_artifact_to_json,
    write_visual_post_artifact,
)


def test_preview_surfaces_failed_alt_text_guard(db):
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Visual launch post",
        eval_score=8.0,
        eval_feedback="Good",
        image_path="/tmp/presence-images/visual.png",
        image_prompt="Launch metrics dashboard",
    )

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["alt_text"]["status"] == "failed"
    assert preview["alt_text"]["required"] is True
    assert preview["alt_text"]["issues"][0]["code"] == "missing_alt_text"
    assert preview["platforms"]["x"]["alt_text"]["status"] == "failed"

    payload = json.loads(preview_to_json(preview))
    assert payload["alt_text"]["issues"][0]["code"] == "missing_alt_text"

    text = format_preview(preview)
    assert "Alt text guard: failed" in text
    assert "- missing_alt_text: Visual posts require alt text before publishing." in text


def test_visual_post_artifact_helpers_write_json_and_markdown(db, tmp_path):
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Reviewable visual post",
        eval_score=8.0,
        eval_feedback="Good",
        image_path="/tmp/presence-images/visual.png",
        image_prompt="ANNOTATED | Launch | Reviewable visual post",
        image_alt_text="Annotated graphic titled Launch with reviewable visual post.",
    )
    preview = build_publication_preview(db, content_id=content_id)
    artifact = {
        "artifact_type": "visual_post_review",
        "generated_at": "2026-04-18T12:00:00+00:00",
        "run": {
            "outcome": "dry_run",
            "planned_topic_id": None,
        },
        "content": {
            "id": content_id,
            "content_type": preview["content"]["content_type"],
            "text": "Reviewable visual post",
            "image_path": "/tmp/presence-images/visual.png",
            "image_prompt": "ANNOTATED | Launch | Reviewable visual post",
            "image_alt_text": "Annotated graphic titled Launch with reviewable visual post.",
        },
        "image": {
            "path": "/tmp/presence-images/visual.png",
            "provider": "pillow",
            "style": "annotated",
            "prompt_used": "annotated: Launch",
            "alt_text": "Annotated graphic titled Launch with reviewable visual post.",
            "spec": "ANNOTATED | Launch | Reviewable visual post",
        },
        "preview": preview,
    }

    assert visual_post_artifact_filename(content_id) == f"visual-post-{content_id}.json"

    json_path = write_visual_post_artifact(
        artifact,
        tmp_path / visual_post_artifact_filename(content_id, artifact_format="json"),
        artifact_format="json",
    )
    markdown_path = write_visual_post_artifact(
        artifact,
        tmp_path / visual_post_artifact_filename(content_id, artifact_format="markdown"),
        artifact_format="markdown",
    )

    payload = json.loads(json_path.read_text())
    assert payload["artifact_type"] == "visual_post_review"
    assert payload["content"]["id"] == content_id
    assert payload["run"]["outcome"] == "dry_run"
    assert payload["preview"]["content"]["id"] == content_id

    markdown = markdown_path.read_text()
    assert markdown.startswith("# Visual Post Review")
    assert "## Final Text" in markdown
    assert "## Publication Preview" in markdown
    assert "Reviewable visual post" in markdown
    assert visual_post_artifact_to_json(artifact).startswith("{")
    assert format_visual_post_artifact(artifact).startswith("# Visual Post Review")


def test_preview_surfaces_passed_alt_text_guard(db):
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Visual launch post",
        eval_score=8.0,
        eval_feedback="Good",
        image_path="/tmp/presence-images/visual.png",
        image_prompt="Launch metrics dashboard",
        image_alt_text="Launch metrics dashboard with trend annotations and labels.",
    )

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["alt_text"]["status"] == "passed"
    assert preview["alt_text"]["issues"] == []
    assert "Alt text guard: passed" in format_preview(preview)


def test_preview_surfaces_restricted_knowledge_license_guard(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Source-backed post",
        eval_score=8.0,
        eval_feedback="Good",
    )
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "restricted-article",
            "https://source.example/restricted",
            "Source Author",
            "Restricted source context",
            "restricted",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["license_guard"]["status"] == "blocked"
    assert preview["license_guard"]["blocked"] is True
    assert preview["license_guard"]["restricted_sources"][0] == {
        "knowledge_id": knowledge_id,
        "source_url": "https://source.example/restricted",
        "license": "restricted",
    }
    assert preview["platforms"]["x"]["license_guard"]["status"] == "blocked"

    payload = json.loads(preview_to_json(preview))
    assert payload["license_guard"]["restricted_sources"][0]["knowledge_id"] == knowledge_id

    text = format_preview(preview)
    assert "License guard: blocked (1 restricted sources)" in text
    assert f"- knowledge {knowledge_id}: restricted https://source.example/restricted" in text


def test_preview_surfaces_attribution_required_guard(db):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Source-backed post without citation",
        eval_score=8.0,
        eval_feedback="Good",
    )
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "attribution-article",
            "https://source.example/attribution",
            "Source Author",
            "Attribution-required source context",
            "attribution_required",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    preview = build_publication_preview(db, content_id=content_id)

    assert preview["attribution_guard"]["status"] == "blocked"
    assert preview["attribution_guard"]["blocked"] is True
    assert preview["attribution_guard"]["missing_sources"][0] == {
        "knowledge_id": knowledge_id,
        "source_url": "https://source.example/attribution",
        "author": "Source Author",
        "license": "attribution_required",
    }
    assert preview["platforms"]["x"]["attribution_guard"]["status"] == "blocked"

    payload = json.loads(preview_to_json(preview))
    assert payload["attribution_guard"]["missing_sources"][0]["knowledge_id"] == knowledge_id

    text = format_preview(preview)
    assert "Attribution guard: blocked (1 missing citations, 1 attribution-required sources)" in text
    assert (
        f"- knowledge {knowledge_id}: attribution_required "
        "Source Author https://source.example/attribution"
    ) in text
