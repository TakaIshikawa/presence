"""Tests for carousel planning exports."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.carousel_export import (  # noqa: E402
    CarouselExportError,
    build_carousel_export,
    build_carousel_export_from_preview,
    carousel_to_json,
    format_carousel_markdown,
)


def _content(content: str, content_id: int = 42) -> dict:
    return {
        "id": content_id,
        "content_type": "x_thread",
        "content": content,
    }


def test_thread_posts_become_slide_titles_and_bullets():
    export = build_carousel_export(
        _content(
            "TWEET 1:\nPlanning exports should start from the thread hook. "
            "Designers need the key point, not the raw tweet.\n\n"
            "TWEET 2:\nEach post becomes a slide. Bullets preserve the supporting details."
        )
    )

    assert export.slide_count == 2
    assert export.slides[0].title == "Planning exports should start from the thread hook"
    assert export.slides[0].body_bullets == [
        "Designers need the key point, not the raw tweet"
    ]
    assert export.slides[1].title == "Each post becomes a slide"
    assert export.slides[1].source_post.startswith("Each post becomes")


def test_slide_count_limit_is_enforced():
    thread = "\n\n".join(
        f"TWEET {index}:\nSlide {index} title. Detail {index}."
        for index in range(1, 7)
    )

    export = build_carousel_export(_content(thread), max_slides=3)

    assert export.slide_count == 3
    assert [slide.index for slide in export.slides] == [1, 2, 3]
    assert export.slides[-1].title == "Slide 3 title"


def test_invalid_slide_limit_raises():
    with pytest.raises(CarouselExportError, match="max_slides must be positive"):
        build_carousel_export(_content("TWEET 1:\nOnly slide"), max_slides=0)


def test_visual_notes_use_existing_prompt_conventions():
    export = build_carousel_export(
        _content(
            "TWEET 1:\nError rate dropped 43% after moving checks earlier. "
            "One metric made the rollout decision obvious.\n\n"
            "TWEET 2:\nBefore: scattered validation. After: one shared path."
        )
    )

    first, second = export.slides
    assert first.visual_prompt_convention.startswith("METRIC |")
    assert "visual post METRIC convention" in first.visual_note
    assert second.visual_prompt_convention.startswith("COMPARISON |")
    assert "visual post COMPARISON convention" in second.visual_note


def test_alt_text_prompt_is_included_for_each_slide():
    export = build_carousel_export(
        _content("TWEET 1:\nShip the outline first. Then design can move faster.")
    )

    prompt = export.slides[0].alt_text_prompt
    assert 'title "Ship the outline first"' in prompt
    assert "body points: Then design can move faster" in prompt
    assert "Keep it under 300 characters" in prompt


def test_preview_payload_can_build_carousel_export():
    preview = {
        "content": {"id": 12, "content_type": "x_thread"},
        "platforms": {
            "x": {
                "posts": [
                    {"text": "Hook from preview. Useful supporting point."},
                    {"text": "Second preview post. Another detail."},
                ]
            }
        },
    }

    export = build_carousel_export_from_preview(preview)

    assert export.content_id == 12
    assert export.source == "publication_preview"
    assert [slide.title for slide in export.slides] == [
        "Hook from preview",
        "Second preview post",
    ]


def test_json_and_markdown_artifacts_include_slide_fields():
    export = build_carousel_export(
        _content("TWEET 1:\nCarousel title. A bullet for design.")
    )

    payload = json.loads(carousel_to_json(export))
    markdown = format_carousel_markdown(export)

    assert payload["slides"][0]["title"] == "Carousel title"
    assert payload["slides"][0]["alt_text_prompt"]
    assert "## Slide 1: Carousel title" in markdown
    assert "### Visual Notes" in markdown
    assert "### Alt Text Prompt" in markdown


def test_export_carousel_cli_writes_json_from_content_id(db, tmp_path, capsys):
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=[],
        source_messages=[],
        content="TWEET 1:\nCLI artifact title. Detail for the slide.",
        eval_score=8.0,
        eval_feedback="Good",
    )
    artifact_path = tmp_path / "carousel.json"

    import export_carousel

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("export_carousel.script_context", return_value=Context()):
        exit_code = export_carousel.main(
            [
                "--content-id",
                str(content_id),
                "--out",
                str(artifact_path),
                "--format",
                "json",
            ]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"Carousel artifact: {artifact_path}" in captured.err
    payload = json.loads(artifact_path.read_text())
    assert payload["content_id"] == content_id
    assert payload["slides"][0]["title"] == "CLI artifact title"


def test_export_carousel_cli_writes_markdown_from_preview_payload(tmp_path, capsys):
    preview_path = tmp_path / "preview.json"
    artifact_path = tmp_path / "carousel.md"
    preview_path.write_text(
        json.dumps(
            {
                "content": {"id": 99, "content_type": "x_thread"},
                "platforms": {
                    "x": {"posts": [{"text": "Preview title. Preview detail."}]}
                },
            }
        )
    )

    import export_carousel

    exit_code = export_carousel.main(
        [
            "--preview-payload",
            str(preview_path),
            "--out",
            str(artifact_path),
            "--format",
            "markdown",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"Carousel artifact: {artifact_path}" in captured.err
    artifact = artifact_path.read_text()
    assert artifact.startswith("# Carousel Slide Outline")
    assert "## Slide 1: Preview title" in artifact
