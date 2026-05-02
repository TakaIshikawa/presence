"""Tests for newsletter source freshness planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.newsletter_source_freshness_planner import (
    build_newsletter_source_freshness_plan,
    format_newsletter_source_freshness_plan_json,
    format_newsletter_source_freshness_plan_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "plan_newsletter_source_freshness.py"
)
spec = importlib.util.spec_from_file_location("plan_newsletter_source_freshness_script", SCRIPT_PATH)
plan_newsletter_source_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(plan_newsletter_source_freshness_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            title TEXT,
            content TEXT,
            created_at TEXT,
            source_url TEXT,
            published_url TEXT,
            source_content_ids TEXT,
            curation_quality TEXT
        );
        CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            subject TEXT,
            status TEXT,
            created_at TEXT,
            sent_at TEXT,
            source_content_ids TEXT
        );
        """
    )
    return conn


def _insert_source(
    conn: sqlite3.Connection,
    source_id: int,
    *,
    title: str,
    created_at: str,
    url: str | None = None,
) -> None:
    conn.execute(
        """INSERT INTO generated_content
           (id, content_type, title, content, created_at, source_url, published_url)
           VALUES (?, 'x_post', ?, ?, ?, ?, ?)""",
        (source_id, title, f"{title}\nBody", created_at, url, url),
    )
    conn.commit()


def _insert_send(
    conn: sqlite3.Connection,
    send_id: int,
    *,
    source_ids: list[int],
    subject: str = "Runtime notes",
    status: str = "draft",
    created_at: str = "2026-05-02T10:00:00+00:00",
) -> None:
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, created_at, sent_at, source_content_ids)
           VALUES (?, ?, ?, ?, ?, NULL, ?)""",
        (send_id, f"issue-{send_id}", subject, status, created_at, json.dumps(source_ids)),
    )
    conn.commit()


def test_fresh_sources_do_not_emit_groups():
    conn = _conn()
    _insert_source(
        conn,
        10,
        title="Fresh release notes",
        created_at="2026-05-01T12:00:00+00:00",
        url="https://example.test/fresh",
    )
    _insert_send(conn, 1, source_ids=[10])

    report = build_newsletter_source_freshness_plan(
        conn,
        now=NOW,
        max_source_age_days=14,
    )

    assert report["totals"]["items_scanned"] == 1
    assert report["totals"]["stale_source_count"] == 0
    assert report["groups"] == []
    assert json.loads(format_newsletter_source_freshness_plan_json(report))["artifact_type"] == (
        "newsletter_source_freshness_planner"
    )


def test_stale_sources_are_grouped_with_source_metadata_and_refresh_queries():
    conn = _conn()
    _insert_source(
        conn,
        20,
        title="Old API launch",
        created_at="2026-03-15T12:00:00+00:00",
        url="https://example.test/old-api",
    )
    _insert_source(
        conn,
        21,
        title="Fresh API launch follow-up",
        created_at="2026-05-01T12:00:00+00:00",
        url="https://example.test/fresh-api",
    )
    _insert_send(conn, 2, source_ids=[20, 21], subject="API launch lessons")

    report = build_newsletter_source_freshness_plan(
        conn,
        now=NOW,
        max_source_age_days=14,
    )
    group = report["groups"][0]
    stale = group["sources"][0]

    assert report["totals"]["stale_item_count"] == 1
    assert report["totals"]["source_count"] == 2
    assert report["totals"]["stale_source_count"] == 1
    assert group["group_type"] == "newsletter_send"
    assert group["item"]["subject"] == "API launch lessons"
    assert stale["source_content_id"] == 20
    assert stale["source_age_days"] == 48.0
    assert stale["source_url"] == "https://example.test/old-api"
    assert stale["source_title"] == "Old API launch"
    assert group["suggestions"] == [
        {
            "group_id": 2,
            "group_type": "newsletter_send",
            "source_content_id": 20,
            "query": "Find current sources for API launch lessons that update or replace Old API launch",
        }
    ]


def test_generated_newsletter_ready_content_is_supported():
    conn = _conn()
    _insert_source(conn, 30, title="Old benchmark", created_at="2026-04-01T12:00:00+00:00")
    conn.execute(
        """INSERT INTO generated_content
           (id, content_type, title, content, created_at, source_content_ids, curation_quality)
           VALUES (100, 'newsletter', 'May benchmark issue', 'Draft', ?, ?, 'ready')""",
        ("2026-05-02T09:00:00+00:00", json.dumps([30])),
    )
    conn.commit()

    report = build_newsletter_source_freshness_plan(
        conn,
        now=NOW,
        max_source_age_days=14,
    )

    assert report["groups"][0]["group_type"] == "generated_content"
    assert report["groups"][0]["item"]["content_id"] == 100
    assert "May benchmark issue" in report["suggestions"][0]["query"]


def test_missing_schema_returns_empty_report_with_diagnostics():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_source_freshness_plan(conn, now=NOW)
    text = format_newsletter_source_freshness_plan_text(report)

    assert report["groups"] == []
    assert report["missing_tables"] == ["newsletter_sends", "generated_content"]
    assert report["totals"]["items_scanned"] == 0
    assert "Missing tables: newsletter_sends, generated_content" in text

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE newsletter_sends (id INTEGER PRIMARY KEY)")
    partial.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")
    missing_columns = build_newsletter_source_freshness_plan(partial, now=NOW)
    assert missing_columns["missing_columns"] == {
        "generated_content": ["created_at"],
        "newsletter_sends": ["source_content_ids"],
    }
    assert missing_columns["groups"] == []


def test_cli_formats_json_and_text(monkeypatch, capsys):
    conn = _conn()
    _insert_source(conn, 40, title="Old observability post", created_at="2026-03-01T12:00:00+00:00")
    _insert_send(conn, 4, source_ids=[40], subject="Observability notes")
    monkeypatch.setattr(
        plan_newsletter_source_freshness_script,
        "script_context",
        lambda: _script_context(conn),
    )
    monkeypatch.setattr(
        plan_newsletter_source_freshness_script,
        "build_newsletter_source_freshness_plan",
        lambda db, **kwargs: build_newsletter_source_freshness_plan(db, now=NOW, **kwargs),
    )

    assert plan_newsletter_source_freshness_script.main(
        ["--days", "7", "--max-source-age-days", "14", "--format", "json"]
    ) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["filters"]["days"] == 7
    assert payload["groups"][0]["group_id"] == 4
    assert payload["suggestions"][0]["source_content_id"] == 40

    assert plan_newsletter_source_freshness_script.main(["--format", "text"]) == 0
    text = capsys.readouterr().out
    assert "Newsletter Source Freshness Plan" in text
    assert "query: Find current sources for Observability notes" in text

    invalid = plan_newsletter_source_freshness_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "value must be positive" in captured.err
