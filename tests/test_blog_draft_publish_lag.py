"""Tests for blog draft publish lag reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.blog_draft_publish_lag import (
    build_blog_draft_publish_lag_report,
    build_blog_draft_publish_lag_report_from_db,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_draft_publish_lag.py"
spec = importlib.util.spec_from_file_location("blog_draft_publish_lag_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_returns_stale_draft_rows_with_age_and_status():
    report = build_blog_draft_publish_lag_report(
        [{"id": 1, "created_at": "2026-04-01T12:00:00+00:00", "title": "Old draft"}],
        stale_days=14,
        now=NOW,
    )

    assert report["stale_drafts"][0]["content_id"] == "1"
    assert report["stale_drafts"][0]["status"] == "stale_unpublished"
    assert report["stale_drafts"][0]["draft_age_days"] == 30.0
    assert report["stale_drafts"][0]["publish_lag_days"] is None


def test_summary_includes_median_max_and_stale_counts_by_topic():
    report = build_blog_draft_publish_lag_report(
        [
            {"id": 1, "created_at": "2026-04-01T00:00:00+00:00", "published_at": "2026-04-06T00:00:00+00:00", "topic": "ai"},
            {"id": 2, "created_at": "2026-04-02T00:00:00+00:00", "published_at": "2026-04-12T00:00:00+00:00", "topic": "ai"},
            {"id": 3, "created_at": "2026-04-01T00:00:00+00:00", "topic": "ai"},
        ],
        stale_days=14,
        now=NOW,
    )

    summary = report["summary_by_group"][0]
    assert summary["group"] == "ai"
    assert summary["count"] == 3
    assert summary["median_lag_days"] == 7.5
    assert summary["max_lag_days"] == 10.0
    assert summary["stale_threshold_count"] == 1


def test_recent_unpublished_draft_is_not_stale():
    report = build_blog_draft_publish_lag_report(
        [{"id": 1, "created_at": "2026-04-30T12:00:00+00:00", "source_type": "curated_article"}],
        stale_days=14,
        now=NOW,
    )

    assert report["drafts"][0]["status"] == "draft"
    assert report["stale_drafts"] == []


def test_db_loader_joins_publications_and_topics():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT, title TEXT, created_at TEXT);
        CREATE TABLE content_publications (id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, published_at TEXT);
        CREATE TABLE content_topics (content_id INTEGER, topic TEXT);
        INSERT INTO generated_content VALUES (1, 'blog_post', 'Draft', '2026-04-01T00:00:00+00:00');
        INSERT INTO content_publications VALUES (10, 1, 'blog', '2026-04-04T00:00:00+00:00');
        INSERT INTO content_topics VALUES (1, 'ops');"""
    )

    report = build_blog_draft_publish_lag_report_from_db(conn, now=NOW)

    assert report["drafts"][0]["publish_lag_days"] == 3.0
    assert report["summary_by_group"][0]["group"] == "ops"


def test_cli_supports_json_and_table(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_blog_draft_publish_lag_report_from_db",
        lambda _db, **kwargs: build_blog_draft_publish_lag_report(
            [{"id": 1, "created_at": "2026-04-01T12:00:00+00:00"}],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--stale-days", "7", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["stale_drafts"][0]["status"] == "stale_unpublished"
    assert script.main(["--format", "table"]) == 0
    assert "content_id | status" in capsys.readouterr().out
