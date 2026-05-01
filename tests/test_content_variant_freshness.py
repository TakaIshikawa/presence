"""Tests for content variant freshness selection."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.content_variant_freshness import (
    ACTION_REFRESH_SELECTED,
    ACTION_REVIEW_LOW_RESONANCE,
    REASON_BASE_CONTENT_NEWER,
    REASON_FEEDBACK_NEWER,
    REASON_LOW_RESONANCE,
    REASON_PERFORMANCE_CONTEXT_NEWER,
    REASON_SELECTED_COPY_NEWER,
    build_content_variant_freshness_report,
    format_content_variant_freshness_json,
    format_content_variant_freshness_text,
    select_stale_content_variants,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_variant_freshness.py"
spec = importlib.util.spec_from_file_location("content_variant_freshness_script", SCRIPT_PATH)
content_variant_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_variant_freshness_script)

NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_content(db, *, content: str = "Original copy") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _set_timestamp(db, table: str, row_id: int, value: str, column: str = "created_at") -> None:
    db.conn.execute(f"UPDATE {table} SET {column} = ? WHERE id = ?", (value, row_id))
    db.conn.commit()


def test_identifies_variant_older_than_base_content_timestamp(db):
    content_id = _insert_content(db)
    variant_id = db.upsert_content_variant(content_id, "x", "post", "Old X copy")
    _set_timestamp(db, "generated_content", content_id, "2026-04-20T10:00:00+00:00")
    _set_timestamp(db, "content_variants", variant_id, "2026-04-19T10:00:00+00:00")

    recommendations = select_stale_content_variants(db, platform="x", days=30, now=NOW)

    assert len(recommendations) == 1
    assert recommendations[0].variant_id == variant_id
    assert recommendations[0].reasons == [REASON_BASE_CONTENT_NEWER]
    assert recommendations[0].action == "refresh_variant"


def test_selected_platform_copy_makes_peer_variant_stale(db):
    content_id = _insert_content(db)
    old_id = db.upsert_content_variant(content_id, "x", "post", "Old post")
    selected_id = db.upsert_content_variant(content_id, "x", "thread", "New selected thread")
    _set_timestamp(db, "generated_content", content_id, "2026-04-18T10:00:00+00:00")
    _set_timestamp(db, "content_variants", old_id, "2026-04-20T10:00:00+00:00")
    _set_timestamp(db, "content_variants", selected_id, "2026-04-22T10:00:00+00:00")
    db.select_content_variant(content_id, "x", "thread")

    recommendations = select_stale_content_variants(db, platform="x", days=30, now=NOW)

    assert [item.variant_id for item in recommendations] == [old_id]
    assert recommendations[0].reasons == [REASON_SELECTED_COPY_NEWER]


def test_feedback_and_low_resonance_prioritize_selected_variant(db):
    content_id = _insert_content(db)
    variant_id = db.upsert_content_variant(content_id, "x", "post", "Selected stale copy")
    db.select_content_variant(content_id, "x", "post")
    feedback_id = db.add_content_feedback(content_id, "revise", "Make it more concrete")
    _set_timestamp(db, "content_variants", variant_id, "2026-04-19T10:00:00+00:00")
    _set_timestamp(db, "content_feedback", feedback_id, "2026-04-23T10:00:00+00:00")
    db.conn.execute(
        "UPDATE generated_content SET auto_quality = ?, published_at = ? WHERE id = ?",
        ("low_resonance", "2026-04-22T10:00:00+00:00", content_id),
    )
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?)""",
        (content_id, "tweet-1", 0.0, "2026-04-24T10:00:00+00:00"),
    )
    db.conn.commit()

    recommendations = select_stale_content_variants(db, platform="x", days=30, now=NOW)

    assert len(recommendations) == 1
    item = recommendations[0]
    assert item.selected is True
    assert item.action == ACTION_REVIEW_LOW_RESONANCE
    assert item.priority > 20
    assert REASON_FEEDBACK_NEWER in item.reasons
    assert REASON_PERFORMANCE_CONTEXT_NEWER in item.reasons
    assert REASON_LOW_RESONANCE in item.reasons


def test_report_formatters_and_dry_run_do_not_modify_variants(db):
    content_id = _insert_content(db)
    variant_id = db.upsert_content_variant(content_id, "bluesky", "post", "Old Bluesky copy")
    _set_timestamp(db, "generated_content", content_id, "2026-04-24T10:00:00+00:00")
    _set_timestamp(db, "content_variants", variant_id, "2026-04-20T10:00:00+00:00")
    before = db.list_content_variants(content_id)

    report = build_content_variant_freshness_report(
        db,
        platform="bluesky",
        days=30,
        mark_stale_dry_run=True,
        now=NOW,
    )
    payload = json.loads(format_content_variant_freshness_json(report))
    text = format_content_variant_freshness_text(report)

    assert payload["artifact_type"] == "content_variant_freshness"
    assert payload["dry_run_plan"][0]["operation"] == "mark_stale"
    assert "would_mark_stale" in text
    assert db.list_content_variants(content_id) == before


def test_days_filter_uses_newest_context_not_variant_age(db):
    content_id = _insert_content(db)
    variant_id = db.upsert_content_variant(content_id, "x", "post", "Very old copy")
    feedback_id = db.add_content_feedback(content_id, "revise", "Recent feedback")
    _set_timestamp(db, "generated_content", content_id, "2026-01-31T10:00:00+00:00")
    _set_timestamp(db, "content_variants", variant_id, "2026-02-01T10:00:00+00:00")
    _set_timestamp(db, "content_feedback", feedback_id, "2026-04-24T10:00:00+00:00")

    recommendations = select_stale_content_variants(db, platform="x", days=7, now=NOW)

    assert [item.variant_id for item in recommendations] == [variant_id]
    assert recommendations[0].reasons == [REASON_FEEDBACK_NEWER]


def test_cli_json_output_uses_db_path_and_dry_run(file_db, capsys):
    content_id = _insert_content(file_db)
    variant_id = file_db.upsert_content_variant(content_id, "x", "post", "Old X copy")
    _set_timestamp(file_db, "generated_content", content_id, "2026-04-24T10:00:00+00:00")
    _set_timestamp(file_db, "content_variants", variant_id, "2026-04-20T10:00:00+00:00")

    exit_code = content_variant_freshness_script.main(
        [
            "--db",
            str(file_db.db_path),
            "--platform",
            "x",
            "--days",
            "30",
            "--format",
            "json",
            "--mark-stale-dry-run",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"]["mark_stale_dry_run"] is True
    assert payload["recommendations"][0]["variant_id"] == variant_id


def test_cli_text_output_uses_script_context(db, monkeypatch, capsys):
    content_id = _insert_content(db)
    variant_id = db.upsert_content_variant(content_id, "x", "post", "Old X copy")
    _set_timestamp(db, "generated_content", content_id, "2026-04-24T10:00:00+00:00")
    _set_timestamp(db, "content_variants", variant_id, "2026-04-20T10:00:00+00:00")
    monkeypatch.setattr(
        content_variant_freshness_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = content_variant_freshness_script.main(["--platform", "x"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Content Variant Freshness" in output
    assert f"variant={variant_id}" in output
    assert ACTION_REFRESH_SELECTED not in output
