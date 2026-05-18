"""Tests for blog series continuation opportunity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.blog_series_continuation_opportunities import (
    build_blog_series_continuation_opportunities_report,
    build_blog_series_continuation_opportunities_report_from_db,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_series_continuation_opportunities.py"
spec = importlib.util.spec_from_file_location("blog_series_continuation_opportunities_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_flags_high_engagement_stale_series_and_dangling_language():
    report = build_blog_series_continuation_opportunities_report(
        [
            {
                "id": "b1",
                "title": "Pipeline Review Part 1",
                "content": "Coming next we will cover rollout details.",
                "series": "Pipeline Review",
                "published_at": "2026-02-01T00:00:00+00:00",
                "status": "published",
            },
            {"id": "b2", "title": "Quiet Post", "published_at": "2026-04-01T00:00:00+00:00", "status": "published"},
        ],
        [{"blog_id": "b1", "engagement_score": 250}, {"blog_id": "b2", "engagement_score": 10}],
        [],
        now=NOW,
    )

    row = report["opportunities"][0]
    assert row["blog_id"] == "b1"
    assert row["title"] == "Pipeline Review Part 1"
    assert row["topic_or_series_key"] == "pipeline-review"
    assert row["published_at"] == "2026-02-01T00:00:00+00:00"
    assert row["engagement_score"] == 250
    assert row["followup_count"] == 0
    assert row["last_followup_at"] is None
    assert row["opportunity_score"] > 250
    assert row["reasons"] == ["high_engagement_no_followup", "stale_series_gap", "dangling_next_step_language"]


def test_recurring_topic_without_series_and_followup_count():
    report = build_blog_series_continuation_opportunities_report(
        [
            {"id": "b1", "title": "Queue Latency Patterns", "topic": "queue latency", "published_at": "2026-03-01T00:00:00+00:00", "status": "published"},
            {"id": "b2", "title": "Queue Latency Lessons", "topic": "queue latency", "published_at": "2026-03-15T00:00:00+00:00", "status": "published"},
        ],
        [{"blog_id": "b1", "views": 1000, "clicks": 40, "likes": 30}],
        [{"id": "f1", "source_blog_id": "b1", "published_at": "2026-04-01T00:00:00+00:00"}],
        high_engagement_score=100,
        now=NOW,
    )

    by_id = {row["blog_id"]: row for row in report["opportunities"]}
    assert by_id["b1"]["followup_count"] == 1
    assert by_id["b1"]["last_followup_at"] == "2026-04-01T00:00:00+00:00"
    assert by_id["b1"]["reasons"] == ["recurring_topic_without_series"]
    assert by_id["b2"]["reasons"] == ["recurring_topic_without_series"]


def test_db_adapter_loads_blog_posts_engagement_and_generated_followups():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE blog_posts (id TEXT, title TEXT, content TEXT, published_at TEXT, status TEXT, series TEXT)")
    conn.execute("CREATE TABLE blog_engagement (blog_id TEXT, engagement_score REAL)")
    conn.execute("CREATE TABLE generated_content (id TEXT, source_blog_id TEXT, published_at TEXT)")
    conn.execute("INSERT INTO blog_posts VALUES ('b1', 'Series Start', 'Next we cover more.', '2026-02-01T00:00:00+00:00', 'published', 'Ops Series')")
    conn.execute("INSERT INTO blog_engagement VALUES ('b1', 200)")

    report = build_blog_series_continuation_opportunities_report_from_db(conn, now=NOW)

    assert report["opportunities"][0]["blog_id"] == "b1"
    assert "dangling_next_step_language" in report["opportunities"][0]["reasons"]


def test_empty_state_and_invalid_filters():
    report = build_blog_series_continuation_opportunities_report([], [], [], now=NOW)

    assert report["empty_state"]["is_empty"] is True
    with pytest.raises(ValueError):
        build_blog_series_continuation_opportunities_report([], high_engagement_score=-1)
    with pytest.raises(ValueError):
        build_blog_series_continuation_opportunities_report([], stale_series_gap_days=0)
    with pytest.raises(ValueError):
        build_blog_series_continuation_opportunities_report([], limit=0)


def test_cli_supports_json_and_text(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_blog_series_continuation_opportunities_report_from_db",
        lambda _db, **kwargs: build_blog_series_continuation_opportunities_report(
            [{"id": "b1", "title": "Series", "series": "Series", "published_at": "2026-01-01T00:00:00+00:00"}],
            [{"blog_id": "b1", "engagement_score": 200}],
            [],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--limit", "1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "blog_series_continuation_opportunities"
    assert script.main(["--table"]) == 0
    assert "blog_id | engagement | followups" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
