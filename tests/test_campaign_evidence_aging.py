"""Tests for campaign evidence aging reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.campaign_evidence_aging import (
    build_campaign_evidence_aging_report,
    format_campaign_evidence_aging_json,
    format_campaign_evidence_aging_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_evidence_aging.py"
spec = importlib.util.spec_from_file_location("campaign_evidence_aging_script", SCRIPT_PATH)
campaign_evidence_aging_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_evidence_aging_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str, *, created_at: str = "2026-05-01T09:00:00+00:00") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at, content_id),
    )
    db.conn.commit()
    return int(content_id)


def _campaign(db, name: str, *, status: str = "active") -> int:
    return int(db.insert_content_campaign(name=name, status=status))


def _planned_topic(db, campaign_id: int, topic: str, *, content_id: int | None = None) -> int:
    topic_id = int(db.insert_planned_topic(topic=topic, campaign_id=campaign_id))
    if content_id is not None:
        db.conn.execute(
            "UPDATE planned_topics SET content_id = ?, status = 'generated' WHERE id = ?",
            (content_id, topic_id),
        )
        db.conn.execute(
            "INSERT INTO content_topics (content_id, topic) VALUES (?, ?)",
            (content_id, topic),
        )
        db.conn.commit()
    return topic_id


def _knowledge(
    db,
    *,
    published_at: str | None,
    ingested_at: str = "2026-04-01T00:00:00+00:00",
    created_at: str = "2026-03-01T00:00:00+00:00",
) -> int:
    row = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, content, approved, published_at, ingested_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            f"source-{published_at or ingested_at}",
            "evidence",
            1,
            published_at,
            ingested_at,
            created_at,
        ),
    )
    db.conn.commit()
    return int(row.lastrowid)


def _link(db, content_id: int, knowledge_id: int) -> None:
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])


def test_fresh_campaign_uses_published_at_before_ingested_at(db):
    campaign_id = _campaign(db, "fresh evidence")
    content_id = _content(db, "fresh content")
    _planned_topic(db, campaign_id, "ai-agents", content_id=content_id)
    knowledge_id = _knowledge(
        db,
        published_at="2026-04-27T10:00:00+00:00",
        ingested_at="2026-01-01T00:00:00+00:00",
    )
    _link(db, content_id, knowledge_id)

    report = build_campaign_evidence_aging_report(db, max_age_days=30, now=NOW)
    payload = json.loads(format_campaign_evidence_aging_json(report))

    assert payload["items"][0]["campaign_id"] == campaign_id
    assert payload["items"][0]["status"] == "fresh"
    assert payload["items"][0]["linked_knowledge_count"] == 1
    assert payload["items"][0]["evidence_age_days"] == 5
    assert payload["items"][0]["topic_coverage"]["topics_with_linked_knowledge"] == 1


def test_stale_campaign_falls_back_to_ingested_at_when_published_at_missing(db):
    campaign_id = _campaign(db, "stale evidence")
    content_id = _content(db, "stale content")
    _planned_topic(db, campaign_id, "developer-experience", content_id=content_id)
    knowledge_id = _knowledge(
        db,
        published_at=None,
        ingested_at="2026-02-01T00:00:00+00:00",
        created_at="2026-01-01T00:00:00+00:00",
    )
    _link(db, content_id, knowledge_id)

    report = build_campaign_evidence_aging_report(
        db,
        max_age_days=30,
        campaign_id=campaign_id,
        status="stale",
        now=NOW,
    )

    assert len(report.items) == 1
    assert report.items[0].status == "stale"
    assert report.items[0].newest_evidence_date == "2026-02-01T00:00:00+00:00"
    assert report.items[0].evidence_age_days == 90
    assert "refresh evidence" in report.items[0].reasons[0]


def test_no_knowledge_links_is_insufficient_with_actionable_reason(db):
    campaign_id = _campaign(db, "missing links")
    content_id = _content(db, "unlinked content")
    _planned_topic(db, campaign_id, "observability", content_id=content_id)

    report = build_campaign_evidence_aging_report(db, campaign_id=campaign_id, now=NOW)
    text = format_campaign_evidence_aging_text(report)

    assert report.items[0].status == "insufficient"
    assert report.items[0].linked_knowledge_count == 0
    assert "Link current knowledge evidence" in report.items[0].reasons[0]
    assert "Campaign Evidence Aging" in text
    assert "knowledge=0" in text


def test_no_planned_topics_is_insufficient_and_cli_status_filter_works(db, monkeypatch, capsys):
    campaign_id = _campaign(db, "empty active campaign")
    monkeypatch.setattr(
        campaign_evidence_aging_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        campaign_evidence_aging_script,
        "build_campaign_evidence_aging_report",
        lambda db, **kwargs: build_campaign_evidence_aging_report(db, now=NOW, **kwargs),
    )

    exit_code = campaign_evidence_aging_script.main(
        [
            "--format",
            "json",
            "--campaign-id",
            str(campaign_id),
            "--status",
            "insufficient",
            "--max-age-days",
            "30",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["items"][0]["campaign_id"] == campaign_id
    assert payload["items"][0]["status"] == "insufficient"
    assert payload["items"][0]["planned_topic_count"] == 0
    assert "Add planned topics" in payload["items"][0]["reasons"][0]
    assert list(payload) == sorted(payload)


def test_missing_optional_evidence_tables_return_valid_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE content_campaigns (
            id INTEGER PRIMARY KEY,
            name TEXT,
            status TEXT,
            start_date TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            topic TEXT,
            content_id INTEGER,
            status TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO content_campaigns (id, name, status, created_at) VALUES (?, ?, ?, ?)",
        (1, "minimal", "active", "2026-05-01T00:00:00+00:00"),
    )
    conn.execute(
        """INSERT INTO planned_topics
           (id, campaign_id, topic, content_id, status)
           VALUES (?, ?, ?, ?, ?)""",
        (10, 1, "testing", None, "planned"),
    )
    conn.commit()

    report = build_campaign_evidence_aging_report(conn, now=NOW)

    assert report.missing_optional_tables == (
        "content_topics",
        "generated_content",
        "content_knowledge_links",
        "knowledge",
    )
    assert report.items[0].status == "insufficient"
    assert report.items[0].planned_topic_count == 1
