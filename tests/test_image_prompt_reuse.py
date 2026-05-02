"""Tests for generated image prompt reuse reporting."""

from __future__ import annotations

import csv
import importlib.util
import io
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from evaluation.image_prompt_reuse import (
    build_image_prompt_reuse_report,
    format_image_prompt_reuse_csv,
    format_image_prompt_reuse_json,
    normalize_image_prompt,
    sequence_similarity,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "image_prompt_reuse.py"
spec = importlib.util.spec_from_file_location("image_prompt_reuse_script", SCRIPT_PATH)
image_prompt_reuse_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(image_prompt_reuse_script)


def _insert_prompt(
    db,
    prompt: str | None,
    *,
    days_ago: int = 1,
    content_type: str = "x_visual",
    image_path: str | None = None,
    image_alt_text: str | None = "Alt text",
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
        image_alt_text=image_alt_text,
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ((NOW - timedelta(days=days_ago)).isoformat(), content_id),
    )
    db.conn.commit()
    return int(content_id)


def test_normalization_and_sequence_similarity_are_stable():
    assert (
        normalize_image_prompt("  Clean, Product-card!! With TEXT  ")
        == "clean product card with text"
    )
    assert sequence_similarity("clean product card", "clean product card") == 1.0
    assert sequence_similarity("", "clean product card") == 0.0
    assert (
        sequence_similarity(
            "clean product dashboard showing deployment progress",
            "clean product dashboard showing release progress",
        )
        > 0.82
    )


def test_exact_duplicate_groups_include_required_content_fields(db):
    first_id = _insert_prompt(
        db,
        "Clean product card with a progress chart",
        image_path="/tmp/one.png",
        image_alt_text="Progress chart showing launch state",
    )
    second_id = _insert_prompt(
        db,
        "clean product card, with a progress chart!",
        content_type="blog_post",
        image_path="/tmp/two.png",
        image_alt_text=None,
    )
    _insert_prompt(db, None)
    _insert_prompt(db, "   ")
    _insert_prompt(db, "Different card with a team timeline")

    report = build_image_prompt_reuse_report(db, now=NOW)
    payload = json.loads(format_image_prompt_reuse_json(report))

    assert report.totals["scanned_prompts"] == 3
    assert report.totals["exact_buckets"] == 1
    finding = payload["findings"][0]
    assert finding["bucket_type"] == "exact"
    assert finding["similarity_bucket"] == "exact"
    assert finding["reuse_count"] == 2
    assert finding["normalized_prompt"] == "clean product card with a progress chart"
    assert [item["content_id"] for item in finding["items"]] == [first_id, second_id]
    assert finding["items"][0]["content_type"] == "x_visual"
    assert finding["items"][0]["image_path"] == "/tmp/one.png"
    assert finding["items"][0]["has_image_alt_text"] is True
    assert finding["items"][1]["content_type"] == "blog_post"
    assert finding["items"][1]["has_image_alt_text"] is False


def test_near_duplicate_groups_use_similarity_threshold(db):
    first_id = _insert_prompt(db, "Clean product dashboard showing deployment progress")
    second_id = _insert_prompt(db, "Clean product dashboard showing release progress")
    _insert_prompt(db, "Watercolor landscape with mountains and a river")

    report = build_image_prompt_reuse_report(
        db,
        similarity_threshold=0.82,
        now=NOW,
    )

    near = [finding for finding in report.findings if finding.bucket_type == "near"]
    assert len(near) == 1
    assert near[0].reuse_count == 2
    assert near[0].min_similarity >= 0.82
    assert {item.content_id for item in near[0].items} == {first_id, second_id}


def test_threshold_and_min_reuse_filter_findings(db):
    _insert_prompt(db, "Clean product dashboard showing deployment progress")
    _insert_prompt(db, "Clean product dashboard showing release progress")
    _insert_prompt(db, "clean product dashboard showing deployment progress")

    high_threshold = build_image_prompt_reuse_report(
        db,
        similarity_threshold=0.99,
        now=NOW,
    )
    min_three = build_image_prompt_reuse_report(
        db,
        min_reuse=3,
        similarity_threshold=0.82,
        now=NOW,
    )

    assert [finding.bucket_type for finding in high_threshold.findings] == ["exact"]
    assert all(finding.reuse_count >= 3 for finding in min_three.findings)


def test_days_filter_and_missing_schema_are_handled(db):
    _insert_prompt(db, "Clean product card")
    _insert_prompt(db, "clean product card", days_ago=40)

    report = build_image_prompt_reuse_report(db, days=7, now=NOW)

    assert report.totals["scanned_prompts"] == 1
    assert report.findings == ()

    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    missing = build_image_prompt_reuse_report(empty, now=NOW)

    assert missing.findings == ()
    assert missing.missing_tables == ("generated_content",)


def test_csv_output_contains_one_row_per_bucket_item(db):
    first_id = _insert_prompt(db, "Clean product card with progress", image_path="/tmp/a.png")
    second_id = _insert_prompt(db, "clean product card with progress", image_alt_text=None)

    report = build_image_prompt_reuse_report(db, now=NOW)
    csv_text = format_image_prompt_reuse_csv(report)
    rows = list(csv.DictReader(io.StringIO(csv_text)))

    assert rows[0]["bucket_id"] == "exact_001"
    assert rows[0]["bucket_type"] == "exact"
    assert rows[0]["similarity_bucket"] == "exact"
    assert rows[0]["reuse_count"] == "2"
    assert rows[0]["content_id"] == str(first_id)
    assert rows[0]["image_path"] == "/tmp/a.png"
    assert rows[0]["has_image_alt_text"] == "true"
    assert rows[1]["content_id"] == str(second_id)
    assert rows[1]["has_image_alt_text"] == "false"
    assert rows[0]["normalized_prompt"] == "clean product card with progress"


def test_invalid_args_raise_value_error(db):
    with pytest.raises(ValueError, match="days must be positive"):
        build_image_prompt_reuse_report(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="min_reuse must be greater than 1"):
        build_image_prompt_reuse_report(db, min_reuse=1, now=NOW)
    with pytest.raises(ValueError, match="similarity_threshold must be between 0 and 1"):
        build_image_prompt_reuse_report(db, similarity_threshold=1.1, now=NOW)


def test_cli_supports_db_json_and_csv_formats(file_db, capsys):
    _insert_prompt(file_db, "Clean product card")
    _insert_prompt(file_db, "clean product card")

    assert (
        image_prompt_reuse_script.main(
            [
                "--db",
                str(file_db.db_path),
                "--days",
                "7",
                "--min-reuse",
                "2",
                "--similarity-threshold",
                "0.82",
                "--format",
                "json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days"] == 7
    assert payload["filters"]["min_reuse"] == 2
    assert payload["filters"]["similarity_threshold"] == 0.82
    assert payload["totals"]["exact_buckets"] == 1

    assert (
        image_prompt_reuse_script.main(
            ["--db", str(file_db.db_path), "--format", "csv"]
        )
        == 0
    )
    assert "bucket_id,bucket_type,similarity_bucket" in capsys.readouterr().out
