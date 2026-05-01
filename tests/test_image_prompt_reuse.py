"""Tests for image prompt reuse auditing."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from image_prompt_reuse import main  # noqa: E402
from synthesis.image_prompt_reuse import (  # noqa: E402
    build_image_prompt_reuse_report,
    format_image_prompt_reuse_text,
    normalize_image_prompt,
    token_overlap_similarity,
)


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _insert_prompt(
    db,
    prompt: str | None,
    *,
    days_ago: float = 1,
    content_type: str = "x_visual",
    published: int = 0,
    image_path: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=f"content for {prompt or 'missing'}",
        eval_score=8.0,
        eval_feedback="ok",
        content_format="image",
        image_path=image_path,
        image_prompt=prompt,
        image_alt_text="Alt text",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ? WHERE id = ?",
        ((BASE_TIME - timedelta(days=days_ago)).isoformat(), published, content_id),
    )
    db.conn.commit()
    return content_id


def test_normalization_and_token_overlap_are_stable():
    assert (
        normalize_image_prompt("  Clean, Product-card!! With TEXT  ")
        == "clean product card with text"
    )
    assert token_overlap_similarity("clean product card", "clean product diagram") == 0.5
    assert token_overlap_similarity("", "clean product diagram") == 0.0


def test_audit_ignores_blank_prompts_and_handles_empty_generated_content(db):
    _insert_prompt(db, None)
    _insert_prompt(db, "   ")

    report = build_image_prompt_reuse_report(db, now=BASE_TIME)

    assert report["totals"]["scanned_prompts"] == 0
    assert report["findings"] == []
    assert report["empty_state"]["is_empty"] is True

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    empty_report = build_image_prompt_reuse_report(conn, now=BASE_TIME)
    assert empty_report["totals"]["scanned_prompts"] == 0
    assert empty_report["empty_state"]["schema_present"] is False


def test_exact_duplicate_groups_include_required_content_fields(db):
    first_id = _insert_prompt(
        db,
        "Clean product card with a progress chart",
        published=1,
        image_path="/tmp/one.png",
    )
    second_id = _insert_prompt(
        db,
        "clean product card, with a progress chart!",
        content_type="x_post",
        image_path="/tmp/two.png",
    )
    _insert_prompt(db, "Different card with a team timeline")

    report = build_image_prompt_reuse_report(db, now=BASE_TIME)

    finding = report["findings"][0]
    assert finding["finding_type"] == "exact_duplicate"
    assert finding["normalized_prompt"] == "clean product card with a progress chart"
    assert [item["content_id"] for item in finding["items"]] == [first_id, second_id]
    assert finding["items"][0]["image_path"] == "/tmp/one.png"
    assert finding["items"][0]["content_type"] == "x_visual"
    assert finding["items"][0]["published"] == "published"
    assert finding["items"][1]["published"] == "unpublished"


def test_near_duplicate_groups_use_similarity_threshold(db):
    _insert_prompt(db, "Clean product dashboard showing deployment progress")
    _insert_prompt(db, "Clean product dashboard showing release progress")
    _insert_prompt(db, "Watercolor landscape with mountains and a river")

    report = build_image_prompt_reuse_report(
        db,
        similarity_threshold=0.6,
        now=BASE_TIME,
    )

    assert report["totals"]["near_duplicate_groups"] == 1
    finding = report["findings"][0]
    assert finding["finding_type"] == "near_duplicate"
    assert finding["count"] == 2
    assert finding["min_similarity"] >= 0.6
    assert {item["content_id"] for item in finding["items"]} == {1, 2}


def test_threshold_filters_near_duplicates(db):
    _insert_prompt(db, "Clean product dashboard showing deployment progress")
    _insert_prompt(db, "Clean product dashboard showing release progress")

    report = build_image_prompt_reuse_report(
        db,
        similarity_threshold=0.9,
        now=BASE_TIME,
    )

    assert report["findings"] == []
    assert report["totals"]["near_duplicate_groups"] == 0


def test_text_formatting_lists_exact_and_near_duplicate_findings(db):
    _insert_prompt(db, "Clean product card with progress")
    _insert_prompt(db, "clean product card with progress")
    _insert_prompt(db, "Annotated incident timeline with owners")
    _insert_prompt(db, "Annotated incident timeline with assignees")

    report = build_image_prompt_reuse_report(
        db,
        similarity_threshold=0.6,
        now=BASE_TIME,
    )
    text = format_image_prompt_reuse_text(report)

    assert "Image prompt reuse audit" in text
    assert "exact_duplicate count=2 similarity=1.00-1.00" in text
    assert "near_duplicate count=2" in text
    assert "id=1 type=x_visual published=unpublished" in text


def test_cli_supports_json_format_and_flags(db, capsys):
    _insert_prompt(db, "Clean product card")
    _insert_prompt(db, "clean product card")
    fixed_report = build_image_prompt_reuse_report(
        db,
        days=7,
        similarity_threshold=0.75,
        limit=5,
        now=BASE_TIME,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("image_prompt_reuse.script_context", fake_script_context), patch(
        "image_prompt_reuse.build_image_prompt_reuse_report",
        return_value=fixed_report,
    ):
        result = main(
            [
                "--days",
                "7",
                "--similarity-threshold",
                "0.75",
                "--limit",
                "5",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["window_days"] == 7
    assert payload["similarity_threshold"] == 0.75
    assert payload["limit"] == 5
