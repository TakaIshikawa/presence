"""Tests for visual post title-card metadata exports."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.visual_title_cards import (  # noqa: E402
    build_recent_visual_title_cards_from_db,
    build_visual_title_card,
    build_visual_title_card_from_artifact,
    visual_title_cards_to_json,
)


def _preview(text: str, *, content_id: int = 42, image_prompt: str = "ANNOTATED | Planned | Body") -> dict:
    return {
        "content": {
            "id": content_id,
            "content_type": "x_visual",
            "image_prompt": image_prompt,
        },
        "platforms": {
            "x": {"posts": [{"text": text}]},
            "bluesky": {"posts": [{"text": text}]},
        },
    }


def test_title_uses_planned_topic_and_caps_length():
    card = build_visual_title_card(
        _preview("Fallback content summary. More context."),
        planned_topic={
            "topic": "A very long planned topic title that should be capped for renderer safety",
            "angle": "Show the practical review angle.",
        },
        max_title_chars=32,
    )

    assert card.title == "A very long planned topic tit..."
    assert card.subtitle == "Show the practical review angle"
    assert len(card.title) <= 32


def test_title_falls_back_to_content_summary_without_planned_topic():
    card = build_visual_title_card(
        _preview("The generated post hook becomes the title. The second sentence helps.")
    )

    assert card.title == "The generated post hook becomes the title"
    assert card.subtitle == "Body"


def test_platform_hints_include_x_and_bluesky_safe_areas():
    card = build_visual_title_card(_preview("Renderer metadata should be stable."))
    payload = json.loads(visual_title_cards_to_json(card))

    assert set(payload["platforms"]) == {"x", "bluesky"}
    assert payload["platforms"]["x"]["aspect_ratio"] == "16:9"
    assert payload["platforms"]["bluesky"]["aspect_ratio"] == "1.91:1"
    assert payload["platforms"]["x"]["safe_area"]["unit"] == "percent"
    assert payload["platforms"]["bluesky"]["safe_area"]["left"] == 9


def test_artifact_builder_reads_preview_and_planned_topic():
    card = build_visual_title_card_from_artifact(
        {
            "run": {
                "planned_topic": {
                    "topic": "Visual review metadata",
                    "angle": "Make downstream rendering deterministic.",
                }
            },
            "preview": _preview("Fallback text."),
        }
    )

    assert card.title == "Visual review metadata"
    assert card.subtitle == "Make downstream rendering deterministic"


def test_recent_db_export_finds_visual_posts_and_linked_topic(db):
    first_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="First visual post should be second.",
        eval_score=8.0,
        eval_feedback="ok",
        image_prompt="METRIC | First | 42% | useful context",
    )
    second_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Second visual post should be newest.",
        eval_score=8.0,
        eval_feedback="ok",
        image_prompt="ANNOTATED | Second | useful body",
    )
    planned_id = db.insert_planned_topic(
        topic="Planned visual topic",
        angle="Use the campaign angle.",
    )
    db.mark_planned_topic_generated(planned_id, second_id)

    cards = build_recent_visual_title_cards_from_db(db, limit=2)

    assert [card.content_id for card in cards] == [second_id, first_id]
    assert cards[0].title == "Planned visual topic"
    assert cards[0].subtitle == "Use the campaign angle"


def test_export_cli_writes_stdout_for_content_id(db, capsys):
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="CLI title comes from generated content.",
        eval_score=8.0,
        eval_feedback="ok",
        image_prompt="ANNOTATED | CLI | subtitle",
    )

    import export_visual_title_cards

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("export_visual_title_cards.script_context", return_value=Context()):
        exit_code = export_visual_title_cards.main(["--content-id", str(content_id)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["content_id"] == content_id
    assert payload["title"] == "CLI title comes from generated content"
    assert captured.err == ""


def test_export_cli_writes_recent_cards_to_output_dir(db, tmp_path, capsys):
    content_id = db.insert_generated_content(
        content_type="x_visual",
        source_commits=[],
        source_messages=[],
        content="Recent title card.",
        eval_score=8.0,
        eval_feedback="ok",
    )

    import export_visual_title_cards

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("export_visual_title_cards.script_context", return_value=Context()):
        exit_code = export_visual_title_cards.main(
            ["--recent", "1", "--out-dir", str(tmp_path)]
        )

    artifact_path = tmp_path / f"visual-title-card-{content_id}.json"
    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"Visual title-card artifact: {artifact_path}" in captured.err
    assert json.loads(artifact_path.read_text())["content_id"] == content_id
