"""Tests for social preview card metadata exports."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.social_preview_cards import (  # noqa: E402
    build_social_preview_card,
    social_preview_cards_to_jsonl,
)


def _row(
    *,
    content_id: int = 1,
    content_type: str = "blog_post",
    content: str = "TITLE: Preview Metadata\n\nThis is the card description.",
    published_url: str | None = "https://example.com/blog/preview-metadata",
    image_path: str | None = "/assets/preview.png",
    image_alt_text: str | None = "A preview image showing metadata fields.",
) -> dict:
    return {
        "id": content_id,
        "content_type": content_type,
        "content": content,
        "published_url": published_url,
        "image_path": image_path,
        "image_alt_text": image_alt_text,
    }


def _insert_published(
    db,
    *,
    content_type: str,
    content: str,
    published_url: str | None = "https://example.com/content",
    image_path: str | None = "/assets/card.png",
    image_alt_text: str | None = "Useful image alt text.",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
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
        (published_url, "2026-04-30T10:00:00+00:00", content_id),
    )
    db.conn.commit()
    return content_id


def test_blog_post_preview_includes_deterministic_social_fields():
    card = build_social_preview_card(_row(content_id=42)).to_dict()

    assert card["content_id"] == 42
    assert card["content_type"] == "blog_post"
    assert card["title"] == "Preview Metadata"
    assert card["description"] == "This is the card description."
    assert card["url"] == "https://example.com/blog/preview-metadata"
    assert card["canonical_url"] == "https://example.com/blog/preview-metadata"
    assert card["image"] == "/assets/preview.png"
    assert card["image_alt_text"] == "A preview image showing metadata fields."
    assert card["warnings"] == []
    assert card["open_graph"]["og:type"] == "article"
    assert card["open_graph"]["og:title"] == "Preview Metadata"
    assert card["twitter_card"]["twitter:card"] == "summary_large_image"
    assert card["platforms"]["twitter"]["twitter:image:alt"] == card["image_alt_text"]


def test_x_visual_preview_uses_post_text_and_website_card_type():
    card = build_social_preview_card(
        _row(
            content_id=7,
            content_type="x_visual",
            content="Visual systems need explicit preview metadata. This keeps renderers deterministic.",
            published_url="https://example.com/visual/7",
        )
    ).to_dict()

    assert card["title"] == "Visual systems need explicit preview metadata."
    assert card["description"] == "This keeps renderers deterministic."
    assert card["open_graph"]["og:type"] == "website"
    assert card["twitter_card"]["twitter:url"] == "https://example.com/visual/7"


def test_missing_url_is_exported_as_warning_metadata():
    card = build_social_preview_card(_row(published_url=None)).to_dict()

    assert card["url"] is None
    assert card["canonical_url"] is None
    assert card["open_graph"]["og:url"] is None
    assert card["warnings"] == [
        {
            "code": "missing_url",
            "field": "published_url",
            "message": "No canonical URL is available for this preview card.",
        }
    ]


def test_missing_alt_text_is_exported_as_warning_metadata():
    card = build_social_preview_card(_row(image_alt_text="  ")).to_dict()

    assert card["image_alt_text"] is None
    assert card["open_graph"]["og:image:alt"] is None
    assert {
        "code": "missing_image_alt_text",
        "field": "image_alt_text",
        "message": "No image alt text is available for this preview card.",
    } in card["warnings"]


def test_missing_image_is_exported_as_warning_metadata():
    card = build_social_preview_card(_row(image_path=None)).to_dict()

    assert card["image"] is None
    assert card["twitter_card"]["twitter:card"] == "summary"
    assert {
        "code": "missing_image",
        "field": "image_path",
        "message": "No preview image is available for this card.",
    } in card["warnings"]


def test_jsonl_serializer_emits_one_record_per_line():
    cards = [
        build_social_preview_card(_row(content_id=1)),
        build_social_preview_card(_row(content_id=2, content_type="x_long_post")),
    ]

    lines = social_preview_cards_to_jsonl(cards).splitlines()

    assert [json.loads(line)["content_id"] for line in lines] == [1, 2]


def test_cli_exports_jsonl_without_mutating_database(db, capsys):
    content_id = _insert_published(
        db,
        content_type="x_visual",
        content="CLI visual preview title. CLI visual preview description.",
        published_url="https://example.com/visual/cli",
    )

    import export_social_preview_cards

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("export_social_preview_cards.script_context", return_value=Context()):
        exit_code = export_social_preview_cards.main(
            [
                "--limit",
                "1",
                "--content-type",
                "x_visual",
                "--output",
                "jsonl",
                "--dry-run",
            ]
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    row = db.conn.execute(
        "SELECT published, published_url FROM generated_content WHERE id = ?",
        (content_id,),
    ).fetchone()

    assert exit_code == 0
    assert payload["content_id"] == content_id
    assert payload["content_type"] == "x_visual"
    assert row["published"] == 1
    assert row["published_url"] == "https://example.com/visual/cli"
    assert "database was not mutated" in captured.err
