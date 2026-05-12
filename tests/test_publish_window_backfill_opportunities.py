"""Tests for publish-window backfill opportunity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.publish_window_backfill_opportunities import (
    build_publish_window_backfill_opportunity_report,
    format_publish_window_backfill_opportunities_json,
    format_publish_window_backfill_opportunities_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "publish_window_backfill_opportunities.py"
)
spec = importlib.util.spec_from_file_location(
    "publish_window_backfill_opportunities_script",
    SCRIPT_PATH,
)
publish_window_backfill_opportunities_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_window_backfill_opportunities_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_type: str = "x_post",
    eval_score: float = 8.0,
    created_days_ago: int = 1,
    published: int = 0,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha"],
        source_messages=["msg"],
        content=f"Generated {content_type}",
        eval_score=eval_score,
        eval_feedback="usable",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ? WHERE id = ?",
        ((NOW - timedelta(days=created_days_ago)).isoformat(), published, content_id),
    )
    db.conn.commit()
    return content_id


def _published_history(db, *, platform: str, when: datetime, content_type: str = "x_post"):
    content_id = _content(db, content_type=content_type, published=1)
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, published_at)
           VALUES (?, ?, 'published', ?)""",
        (content_id, platform, when.isoformat()),
    )
    db.conn.commit()
    return content_id


def test_detects_empty_window_and_ranks_unpublished_candidates(db):
    target = (NOW + timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
    _published_history(
        db,
        platform="x",
        when=target - timedelta(weeks=1),
        content_type="x_post",
    )
    best = _content(db, content_type="x_post", eval_score=9.0, created_days_ago=1)
    stale = _content(db, content_type="x_post", eval_score=9.0, created_days_ago=20)
    wrong_platform = _content(db, content_type="blog_post", eval_score=10.0)
    low_score = _content(db, content_type="x_post", eval_score=5.0)

    report = build_publish_window_backfill_opportunity_report(
        db,
        days_ahead=3,
        min_score=7.0,
        limit=3,
        now=NOW,
    )
    payload = json.loads(format_publish_window_backfill_opportunities_json(report))
    opportunity = payload["opportunities"][0]

    assert payload["artifact_type"] == "publish_window_backfill_opportunities"
    assert payload["totals"]["opportunity_count"] == 1
    assert opportunity["reason"] == "empty_window"
    assert opportunity["window"]["start_time"] == target.isoformat()
    assert [row["content_id"] for row in opportunity["recommended_content"]] == [best, stale]
    assert wrong_platform not in [row["content_id"] for row in opportunity["recommended_content"]]
    assert low_score not in [row["content_id"] for row in opportunity["recommended_content"]]


def test_suitable_queue_suppresses_window_but_low_score_queue_is_underfilled(db):
    target = (NOW + timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    _published_history(db, platform="x", when=target - timedelta(weeks=1))
    candidate = _content(db, eval_score=8.5)
    low_queued = _content(db, eval_score=4.0)
    db.queue_for_publishing(low_queued, target.isoformat(), platform="x")

    report = build_publish_window_backfill_opportunity_report(
        db,
        days_ahead=3,
        min_score=7.0,
        limit=5,
        now=NOW,
    )

    assert report["opportunities"][0]["reason"] == "underfilled_window"
    assert report["opportunities"][0]["queued_count"] == 1
    assert report["opportunities"][0]["recommended_content"][0]["content_id"] == candidate

    suitable = _content(db, eval_score=8.0)
    db.queue_for_publishing(suitable, target.isoformat(), platform="x")
    filled = build_publish_window_backfill_opportunity_report(
        db,
        days_ahead=3,
        min_score=7.0,
        limit=5,
        now=NOW,
    )
    assert filled["opportunities"] == []


def test_missing_schedule_table_returns_clear_empty_state():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            eval_score REAL,
            published INTEGER
        )"""
    )

    report = build_publish_window_backfill_opportunity_report(conn, now=NOW)

    assert report["availability"]["publish_queue_available"] is False
    assert report["missing_tables"] == ["publish_queue", "content_publications"]
    assert report["totals"]["opportunity_count"] == 0
    assert report["opportunities"] == []


def test_text_json_and_cli_flags_are_deterministic(db, monkeypatch, capsys):
    target = (NOW + timedelta(days=1)).replace(hour=14, minute=0, second=0, microsecond=0)
    _published_history(db, platform="x", when=target - timedelta(weeks=1))
    content_id = _content(db, eval_score=8.0)
    monkeypatch.setattr(
        publish_window_backfill_opportunities_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publish_window_backfill_opportunities_script,
        "build_publish_window_backfill_opportunity_report",
        lambda db, **kwargs: build_publish_window_backfill_opportunity_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    report = build_publish_window_backfill_opportunity_report(db, now=NOW)
    payload = json.loads(format_publish_window_backfill_opportunities_json(report))
    text = format_publish_window_backfill_opportunities_text(report)

    assert list(payload) == sorted(payload)
    assert "Publish Window Backfill Opportunities" in text
    assert f"recommendations={content_id}" in text
    assert publish_window_backfill_opportunities_script.main(["--days-ahead", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = publish_window_backfill_opportunities_script.main(
        ["--days-ahead", "3", "--min-score", "7", "--limit", "1", "--format", "json"]
    )
    cli_payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert cli_payload["filters"]["days_ahead"] == 3
    assert cli_payload["filters"]["limit"] == 1
