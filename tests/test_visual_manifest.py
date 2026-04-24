"""Tests for generated visual asset manifests."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

from output.visual_manifest import (
    VisualManifestFilters,
    image_dimensions,
    list_visual_manifest_entries,
    manifest_to_json,
    manifest_to_table,
)


PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


def _insert_visual(
    db,
    *,
    image_path: str,
    alt_text: str | None = "Launch metrics dashboard with conversion trend annotations.",
    prompt: str | None = "Launch metrics dashboard with conversion trend annotations",
    content_type: str = "x_visual",
) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=["abc123"],
        source_messages=["msg-1"],
        source_activity_ids=["42"],
        content="Visual launch post",
        eval_score=8.0,
        eval_feedback="ok",
        content_format="annotated",
        image_path=image_path,
        image_prompt=prompt,
        image_alt_text=alt_text,
    )


def test_manifest_includes_one_entry_per_generated_content_row_with_image_path(db, tmp_path):
    image_path = tmp_path / "visual.png"
    image_path.write_bytes(PNG_1X1)
    content_id = _insert_visual(db, image_path=str(image_path))
    db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="No image",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.upsert_publication_success(
        content_id,
        "x",
        platform_post_id="tw-1",
        published_at="2026-04-24T12:00:00+00:00",
    )

    entries = list_visual_manifest_entries(db)

    assert [entry["content_id"] for entry in entries] == [content_id]
    entry = entries[0]
    assert entry["content_type"] == "x_visual"
    assert entry["created_at"]
    assert entry["prompt_present"] is True
    assert entry["alt_text_status"] == "passed"
    assert entry["alt_text_usable"] is True
    assert entry["dimensions"] == {"width": 1, "height": 1}
    assert entry["source_content_ids"] == {
        "source_commits": ["abc123"],
        "source_messages": ["msg-1"],
        "source_activity_ids": ["42"],
        "repurposed_from": None,
    }
    assert entry["publication"]["status"] == "published"
    assert entry["publication"]["published"] is True
    assert entry["publication"]["tweet_id"] is None


def test_missing_alt_only_filters_visual_assets_without_usable_alt_text(db, tmp_path):
    good_id = _insert_visual(db, image_path=str(tmp_path / "good.png"))
    missing_id = _insert_visual(
        db,
        image_path=str(tmp_path / "missing.png"),
        alt_text="",
    )
    generic_id = _insert_visual(
        db,
        image_path=str(tmp_path / "generic.png"),
        alt_text="A screenshot",
    )

    entries = list_visual_manifest_entries(
        db,
        VisualManifestFilters(missing_alt_only=True),
    )

    assert [entry["content_id"] for entry in entries] == [generic_id, missing_id]
    assert good_id not in {entry["content_id"] for entry in entries}
    assert {entry["alt_text_status"] for entry in entries} == {"failed"}


def test_manifest_filters_by_since_days_and_content_id(db, tmp_path):
    old_id = _insert_visual(db, image_path=str(tmp_path / "old.png"))
    recent_id = _insert_visual(db, image_path=str(tmp_path / "recent.png"))
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ("2026-04-01T00:00:00+00:00", old_id),
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ("2026-04-24T00:00:00+00:00", recent_id),
    )
    db.conn.commit()

    entries = list_visual_manifest_entries(
        db,
        VisualManifestFilters(since_days=7),
        now=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )
    assert [entry["content_id"] for entry in entries] == [recent_id]

    entries = list_visual_manifest_entries(
        db,
        VisualManifestFilters(content_id=old_id),
    )
    assert [entry["content_id"] for entry in entries] == [old_id]


def test_manifest_formats_json_and_table(db, tmp_path):
    content_id = _insert_visual(
        db,
        image_path=str(tmp_path / "visual.png"),
        prompt=None,
    )

    entries = list_visual_manifest_entries(db)
    payload = json.loads(manifest_to_json(entries))
    table = manifest_to_table(entries)

    assert payload[0]["content_id"] == content_id
    assert payload[0]["prompt_present"] is False
    assert "CID" in table
    assert "IMAGE_PATH" in table
    assert "x_visual" in table


def test_image_dimensions_returns_none_when_file_is_unavailable(tmp_path):
    assert image_dimensions(str(tmp_path / "missing.png")) is None
