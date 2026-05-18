"""Tests for blog-to-X repurposing opportunity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.blog_to_x_repurposing_opportunities import (
    build_blog_to_x_repurposing_opportunities_report,
    build_blog_to_x_repurposing_opportunities_report_from_db,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_to_x_repurposing_opportunities.py"
spec = importlib.util.spec_from_file_location("blog_to_x_repurposing_opportunities_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_excludes_blogs_matched_by_source_id_and_url():
    report = build_blog_to_x_repurposing_opportunities_report(
        [
            {"id": "b1", "title": "Durable Pipeline Lessons", "published_url": "https://site.test/blog/pipeline", "published_at": "2026-04-01T00:00:00+00:00"},
            {"id": "b2", "title": "Unshared Notes", "published_url": "https://site.test/blog/unshared", "published_at": "2026-04-01T00:00:00+00:00"},
        ],
        [
            {"id": "x1", "content_type": "x_post", "source_content_ids": '["b1"]', "content": "Read https://site.test/blog/pipeline", "published_at": "2026-04-05T00:00:00+00:00"},
        ],
        now=NOW,
    )

    assert [row["blog_id"] for row in report["opportunities"]] == ["b2"]
    assert report["totals"]["fully_repurposed_count"] == 1
    assert "missing_x_post" in report["opportunities"][0]["reason_codes"]


def test_matches_by_meaningful_title_overlap_and_date_proximity():
    report = build_blog_to_x_repurposing_opportunities_report(
        [{"id": "b1", "title": "Pipeline Latency Budgeting Patterns", "published_at": "2026-04-01T00:00:00+00:00"}],
        [{"id": "x1", "content_type": "x_post", "content": "Pipeline latency budgeting patterns that worked", "published_at": "2026-04-03T00:00:00+00:00"}],
        min_title_token_overlap=3,
        now=NOW,
    )

    assert report["opportunities"] == []
    assert report["totals"]["fully_repurposed_count"] == 1


def test_require_thread_keeps_partial_post_coverage():
    report = build_blog_to_x_repurposing_opportunities_report(
        [{"id": "b1", "title": "Pipeline Latency Budgeting Patterns", "published_at": "2026-04-01T00:00:00+00:00"}],
        [{"id": "x1", "content_type": "x_post", "source_content_ids": "b1", "published_at": "2026-04-02T00:00:00+00:00"}],
        require_thread=True,
        now=NOW,
    )

    row = report["opportunities"][0]
    assert row["matched_x_post_count"] == 1
    assert row["matched_x_thread_count"] == 0
    assert "missing_x_thread" in row["reason_codes"]
    assert report["totals"]["partial_repurposed_count"] == 1


def test_empty_state_and_invalid_filters():
    report = build_blog_to_x_repurposing_opportunities_report([], [], now=NOW)

    assert report["empty_state"]["is_empty"] is True
    assert report["totals"]["blog_count"] == 0
    with pytest.raises(ValueError):
        build_blog_to_x_repurposing_opportunities_report([], [], window_days=0)
    with pytest.raises(ValueError):
        build_blog_to_x_repurposing_opportunities_report([], [], min_title_token_overlap=0)
    with pytest.raises(ValueError):
        build_blog_to_x_repurposing_opportunities_report([], [], limit=0)


def test_db_adapter_loads_generated_content_blogs_and_x_content():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE generated_content (
            id TEXT, content_type TEXT, title TEXT, content TEXT, published INTEGER,
            published_url TEXT, published_at TEXT, source_content_ids TEXT
        )"""
    )
    conn.execute("INSERT INTO generated_content VALUES ('b1', 'blog_post', 'Blog Title', 'Body', 1, 'https://site.test/b1', '2026-04-01T00:00:00+00:00', NULL)")
    conn.execute("INSERT INTO generated_content VALUES ('x1', 'x_post', NULL, 'See https://site.test/b1', 1, NULL, '2026-04-02T00:00:00+00:00', NULL)")

    report = build_blog_to_x_repurposing_opportunities_report_from_db(conn, now=NOW)

    assert report["totals"]["fully_repurposed_count"] == 1
    assert report["opportunities"] == []


def test_cli_supports_json_text_table_and_flags(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_blog_to_x_repurposing_opportunities_report_from_db",
        lambda _db, **kwargs: build_blog_to_x_repurposing_opportunities_report(
            [{"id": "b1", "title": "Blog", "published_at": "2026-01-01T00:00:00+00:00"}],
            [],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--window-days", "10", "--min-title-token-overlap", "1", "--limit", "1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "blog_to_x_repurposing_opportunities"
    assert script.main(["--require-thread", "--table"]) == 0
    assert "blog_id | age_days" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
