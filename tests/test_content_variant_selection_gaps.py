"""Tests for content variant selection gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.content_variant_selection_gaps import (
    BUCKET_MISSING_SELECTION,
    BUCKET_MULTIPLE_SELECTED,
    BUCKET_STALE_UNSELECTED,
    build_content_variant_selection_gap_report,
    format_content_variant_selection_gaps_json,
    format_content_variant_selection_gaps_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_variant_selection_gaps.py"
spec = importlib.util.spec_from_file_location("content_variant_selection_gaps_script", SCRIPT_PATH)
content_variant_selection_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_variant_selection_gaps_script)

NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_content(db, *, content: str = "Generated copy") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _set_variant_created_at(db, variant_id: int, value: str) -> None:
    db.conn.execute(
        "UPDATE content_variants SET created_at = ? WHERE id = ?",
        (value, variant_id),
    )
    db.conn.commit()


def test_clean_data_returns_empty_report(db):
    content_id = _insert_content(db)
    db.upsert_content_variant(content_id, "x", "post", "X copy")
    db.select_content_variant(content_id, "x", "post")

    report = build_content_variant_selection_gap_report(db, now=NOW)

    assert report["findings"] == []
    assert report["totals"]["all"] == {
        BUCKET_MISSING_SELECTION: 0,
        BUCKET_MULTIPLE_SELECTED: 0,
        BUCKET_STALE_UNSELECTED: 0,
    }


def test_missing_selection_is_reported_by_platform(db):
    content_id = _insert_content(db)
    variant_id = db.upsert_content_variant(content_id, "bluesky", "post", "Bluesky copy")

    report = build_content_variant_selection_gap_report(db, now=NOW)

    assert report["totals"]["by_platform"]["bluesky"][BUCKET_MISSING_SELECTION] == 1
    assert report["findings"][0]["bucket"] == BUCKET_MISSING_SELECTION
    assert report["findings"][0]["content_id"] == content_id
    assert report["findings"][0]["variant_ids"] == [variant_id]
    assert report["findings"][0]["selected_count"] == 0


def test_duplicate_selected_rows_are_reported(db):
    content_id = _insert_content(db)
    post_id = db.upsert_content_variant(content_id, "x", "post", "Post copy")
    thread_id = db.upsert_content_variant(content_id, "x", "thread", "Thread copy")
    db.conn.execute("DROP INDEX IF EXISTS idx_content_variants_selected")
    db.conn.execute(
        "UPDATE content_variants SET selected = 1 WHERE id IN (?, ?)",
        (post_id, thread_id),
    )
    db.conn.commit()

    report = build_content_variant_selection_gap_report(db, now=NOW)

    finding = report["findings"][0]
    assert finding["bucket"] == BUCKET_MULTIPLE_SELECTED
    assert finding["selected_count"] == 2
    assert finding["selected_variant_ids"] == [post_id, thread_id]


def test_stale_unselected_variants_are_reported(db):
    content_id = _insert_content(db)
    stale_id = db.upsert_content_variant(content_id, "linkedin", "post", "Old copy")
    selected_id = db.upsert_content_variant(content_id, "linkedin", "summary", "Selected copy")
    db.select_content_variant(content_id, "linkedin", "summary")
    _set_variant_created_at(db, stale_id, "2026-03-20T12:00:00+00:00")
    _set_variant_created_at(db, selected_id, "2026-05-01T12:00:00+00:00")

    report = build_content_variant_selection_gap_report(db, days=30, now=NOW)

    finding = report["findings"][0]
    assert finding["bucket"] == BUCKET_STALE_UNSELECTED
    assert finding["variant_count"] == 2
    assert finding["selected_count"] == 1
    assert finding["stale_variant_ids"] == [stale_id]
    assert finding["oldest_unselected_at"] == "2026-03-20T12:00:00+00:00"


def test_platform_filter_limits_findings(db):
    x_id = _insert_content(db, content="X content")
    bluesky_id = _insert_content(db, content="Bluesky content")
    db.upsert_content_variant(x_id, "x", "post", "X copy")
    db.upsert_content_variant(bluesky_id, "bluesky", "post", "Bluesky copy")

    report = build_content_variant_selection_gap_report(db, platform="x", now=NOW)

    assert [finding["platform"] for finding in report["findings"]] == ["x"]
    assert report["findings"][0]["content_id"] == x_id


def test_json_and_text_formatting_are_stable(db):
    content_id = _insert_content(db)
    db.upsert_content_variant(content_id, "x", "post", "X copy")

    report = build_content_variant_selection_gap_report(db, days=14, now=NOW)
    payload = json.loads(format_content_variant_selection_gaps_json(report))
    text = format_content_variant_selection_gaps_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["generated_at"] == "2026-05-02T12:00:00+00:00"
    assert payload["window_days"] == 14
    assert "Content Variant Selection Gaps" in text
    assert f"content_id={content_id}" in text
    assert "platform=x" in text
    assert "variant_count=1" in text
    assert "selected_count=0" in text
    assert "action=select one variant before publishing" in text


def test_cli_supports_days_platform_and_json_output(db, monkeypatch, capsys):
    content_id = _insert_content(db)
    db.upsert_content_variant(content_id, "x", "post", "X copy")
    db.upsert_content_variant(content_id, "bluesky", "post", "Bluesky copy")
    monkeypatch.setattr(
        content_variant_selection_gaps_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = content_variant_selection_gaps_script.main(
        ["--days", "7", "--platform", "x", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["window_days"] == 7
    assert [finding["platform"] for finding in payload["findings"]] == ["x"]
    assert payload["findings"][0]["content_id"] == content_id
