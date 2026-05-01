"""Tests for automatic content variant selection."""

from __future__ import annotations

import json
from contextlib import contextmanager

from output.content_variant_selector import select_content_variant
from scripts import select_content_variant as select_content_variant_script


@contextmanager
def _script_context(db):
    yield None, db


def _content(
    db,
    sample_content,
    *,
    text: str = "Original copy",
    content_type: str = "x_post",
    content_format: str | None = "tip",
) -> int:
    return db.insert_generated_content(
        **{
            **sample_content,
            "content": text,
            "content_type": content_type,
            "content_format": content_format,
        }
    )


def _variant(
    db,
    content_id: int,
    variant_type: str,
    text: str,
    *,
    platform: str = "x",
    selected: bool = False,
    created_at: str | None = None,
) -> int:
    variant_id = db.upsert_content_variant(
        content_id,
        platform,
        variant_type,
        text,
    )
    if selected:
        db.select_content_variant(content_id, platform, variant_type)
    if created_at:
        db.conn.execute(
            "UPDATE content_variants SET created_at = ? WHERE id = ?",
            (created_at, variant_id),
        )
        db.conn.commit()
    return variant_id


def test_dry_run_returns_ranked_candidates_with_score_components_without_writes(
    db,
    sample_content,
):
    old_content = _content(db, sample_content, text="Historical tip")
    _variant(db, old_content, "thread", "Historical thread", selected=True)
    db.insert_engagement(old_content, "tweet-1", 1, 1, 1, 1, 40.0)

    target_id = _content(db, sample_content, text="Target tip")
    post_id = _variant(
        db,
        target_id,
        "post",
        "Short post",
        selected=True,
        created_at="2026-04-30T10:00:00+00:00",
    )
    thread_id = _variant(
        db,
        target_id,
        "thread",
        "Thread copy",
        created_at="2026-05-01T10:00:00+00:00",
    )

    result = select_content_variant(db, content_id=target_id, platform="x")

    assert result["apply"] is False
    assert result["selected_variant_id"] == thread_id
    assert result["candidates"][0]["variant_type"] == "thread"
    assert result["candidates"][0]["components"]["historical_engagement"] == 8.0
    assert set(result["candidates"][0]["components"]) == {
        "platform_match",
        "variant_type",
        "selected_state",
        "historical_engagement",
        "freshness",
    }
    assert db.get_selected_content_variant(target_id, "x")["id"] == post_id


def test_apply_mode_marks_exactly_one_variant_selected(db, sample_content):
    content_id = _content(db, sample_content, content_type="x_thread")
    _variant(db, content_id, "post", "Post copy", selected=True)
    thread_id = _variant(db, content_id, "thread", "Thread copy")

    result = select_content_variant(
        db,
        content_id=content_id,
        platform="x",
        apply=True,
    )

    selected_rows = [
        row
        for row in db.list_content_variants(content_id, platform="x")
        if row["selected"]
    ]
    assert result["selected_variant_id"] == thread_id
    assert [row["id"] for row in selected_rows] == [thread_id]


def test_selector_falls_back_when_no_historical_engagement_exists(db, sample_content):
    content_id = _content(db, sample_content, content_type="x_thread")
    _variant(db, content_id, "post", "Post copy")
    thread_id = _variant(db, content_id, "thread", "Thread copy")

    result = select_content_variant(db, content_id=content_id, platform="x")

    assert result["history_fallback"] is True
    assert result["selected_variant_id"] == thread_id
    assert all(
        candidate["components"]["historical_engagement"] == 0.0
        for candidate in result["candidates"]
    )


def test_missing_content_or_platform_variants_raise_clear_errors(db, sample_content):
    content_id = _content(db, sample_content)
    _variant(db, content_id, "post", "Post copy", platform="bluesky")

    try:
        select_content_variant(db, content_id=9999, platform="x")
    except ValueError as exc:
        assert "generated_content id 9999 does not exist" in str(exc)
    else:
        raise AssertionError("missing content should raise ValueError")

    try:
        select_content_variant(db, content_id=content_id, platform="x")
    except ValueError as exc:
        assert (
            f"no eligible content variants for content_id={content_id}, platform=x"
            in str(exc)
        )
    else:
        raise AssertionError("missing platform variants should raise ValueError")


def test_cli_json_apply_outputs_selection_and_writes(
    db,
    sample_content,
    monkeypatch,
    capsys,
):
    content_id = _content(db, sample_content, content_type="x_thread")
    _variant(db, content_id, "post", "Post copy")
    thread_id = _variant(db, content_id, "thread", "Thread copy")
    monkeypatch.setattr(
        select_content_variant_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = select_content_variant_script.main(
        [
            "--content-id",
            str(content_id),
            "--platform",
            "x",
            "--apply",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["selected_variant_id"] == thread_id
    assert db.get_selected_content_variant(content_id, "x")["id"] == thread_id
