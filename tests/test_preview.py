"""Tests for publication preview alt-text guard output."""

import json

from output.preview import build_publication_preview, format_preview, preview_to_json


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
