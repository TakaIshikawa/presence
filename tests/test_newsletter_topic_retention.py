"""Tests for newsletter topic retention reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.newsletter_topic_retention import (
    build_newsletter_topic_retention_report,
    format_newsletter_topic_retention_json,
    format_newsletter_topic_retention_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "newsletter_topic_retention.py"
)
spec = importlib.util.spec_from_file_location("newsletter_topic_retention", SCRIPT_PATH)
newsletter_topic_retention = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_topic_retention)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str = "blog_post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, 7.0, 1)""",
        (f"{content_type} body", content_type),
    ).lastrowid


def _send(
    db,
    *,
    issue_id: str,
    content_ids: list[int],
    subscriber_count: int = 100,
    sent_at: datetime,
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Subject {issue_id}",
        content_ids=content_ids,
        subscriber_count=subscriber_count,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at.isoformat(), send_id),
    )
    db.conn.commit()
    return send_id


def _subscriber_snapshot(db, count: int, fetched_at: datetime) -> None:
    db.conn.execute(
        """INSERT INTO newsletter_subscriber_metrics
           (subscriber_count, active_subscriber_count, unsubscribes, fetched_at)
           VALUES (?, ?, 0, ?)""",
        (count, count, fetched_at.isoformat()),
    )
    db.conn.commit()


def test_full_metrics_rank_topics_by_retention_and_engagement(db):
    ai_content = _content(db)
    ops_content = _content(db)
    db.insert_content_topics(ai_content, [("AI", "agents", 1.0)])
    db.insert_content_topics(ops_content, [("Ops", "queues", 1.0)])
    first_sent = NOW - timedelta(days=4)
    second_sent = NOW - timedelta(days=2)
    ai_first = _send(db, issue_id="ai-1", content_ids=[ai_content], sent_at=first_sent)
    ai_second = _send(db, issue_id="ai-2", content_ids=[ai_content], sent_at=second_sent)
    ops_send = _send(db, issue_id="ops-1", content_ids=[ops_content], sent_at=NOW - timedelta(days=1))

    _subscriber_snapshot(db, 100, first_sent - timedelta(hours=1))
    _subscriber_snapshot(db, 108, first_sent + timedelta(hours=2))
    _subscriber_snapshot(db, 108, second_sent - timedelta(hours=1))
    _subscriber_snapshot(db, 112, second_sent + timedelta(hours=2))
    _subscriber_snapshot(db, 112, NOW - timedelta(days=1, hours=1))
    _subscriber_snapshot(db, 109, NOW - timedelta(days=1) + timedelta(hours=2))

    db.insert_newsletter_engagement(ai_first, "ai-1", opens=55, clicks=12, unsubscribes=0)
    db.insert_newsletter_engagement(ai_second, "ai-2", opens=60, clicks=15, unsubscribes=1)
    db.insert_newsletter_engagement(ops_send, "ops-1", opens=30, clicks=3, unsubscribes=2)
    fetched = NOW.isoformat()
    db.insert_newsletter_link_clicks(
        ai_first,
        "ai-1",
        [{"url": "https://example.com/ai-1", "clicks": 10, "unique_clicks": 8}],
        fetched_at=fetched,
    )
    db.insert_newsletter_link_clicks(
        ai_second,
        "ai-2",
        [{"url": "https://example.com/ai-2", "clicks": 11, "unique_clicks": 9}],
        fetched_at=fetched,
    )
    db.insert_newsletter_link_clicks(
        ops_send,
        "ops-1",
        [{"url": "https://example.com/ops", "clicks": 2, "unique_clicks": 2}],
        fetched_at=fetched,
    )

    report = build_newsletter_topic_retention_report(
        db,
        lookback_issues=3,
        min_sends=1,
        now=NOW,
    )

    assert [row.topic for row in report.rows] == ["ai", "ops"]
    ai = report.rows[0]
    assert ai.issue_count == 2
    assert ai.sends == 200
    assert ai.opens == 115
    assert ai.clicks == 21
    assert ai.unsubscribes == 1
    assert ai.retention_delta == 12
    assert ai.recommendation == "grow_topic"
    assert ai.availability == {
        "topics": True,
        "engagement": True,
        "link_clicks": True,
        "subscriber_metrics": True,
    }


def test_missing_optional_tables_produces_partial_rows_with_metadata():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            subject TEXT,
            source_content_ids TEXT,
            subscriber_count INTEGER,
            sent_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT
        )"""
    )
    conn.execute("INSERT INTO generated_content (id, content_type) VALUES (10, 'x_thread')")
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, source_content_ids, subscriber_count, sent_at)
           VALUES (1, 'issue-1', 'Subject', '[10]', 50, ?)""",
        (NOW.isoformat(),),
    )
    conn.commit()
    try:
        report = build_newsletter_topic_retention_report(
            conn,
            lookback_issues=5,
            min_sends=1,
            now=NOW,
        )
    finally:
        conn.close()

    row = report.rows[0]
    assert row.topic == "x-thread"
    assert row.topic_kind == "section"
    assert row.opens is None
    assert row.clicks is None
    assert row.retention_delta is None
    assert row.availability["engagement"] is False
    assert report.missing_tables == (
        "content_topics",
        "newsletter_engagement",
        "newsletter_link_clicks",
        "newsletter_subscriber_metrics",
    )


def test_low_sample_topics_are_marked_by_min_sends(db):
    content_id = _content(db)
    db.insert_content_topics(content_id, [("Testing", "pytest", 1.0)])
    send_id = _send(
        db,
        issue_id="testing-1",
        content_ids=[content_id],
        subscriber_count=100,
        sent_at=NOW - timedelta(days=1),
    )
    db.insert_newsletter_engagement(send_id, "testing-1", opens=50, clicks=5, unsubscribes=0)

    report = build_newsletter_topic_retention_report(
        db,
        lookback_issues=1,
        min_sends=2,
        now=NOW,
    )

    assert report.rows[0].sample_status == "low_sample"
    assert report.rows[0].recommendation == "collect_more_data"
    assert report.totals["low_sample_count"] == 1


def test_json_text_and_cli_outputs_are_deterministic(db, capsys):
    content_id = _content(db)
    db.insert_content_topics(content_id, [("Delivery", "timing", 1.0)])
    send_id = _send(
        db,
        issue_id="delivery-1",
        content_ids=[content_id],
        subscriber_count=100,
        sent_at=NOW - timedelta(days=1),
    )
    db.insert_newsletter_engagement(send_id, "delivery-1", opens=45, clicks=6, unsubscribes=0)
    db.insert_newsletter_link_clicks(
        send_id,
        "delivery-1",
        [{"url": "https://example.com/delivery", "clicks": 6, "unique_clicks": 5}],
        fetched_at=NOW.isoformat(),
    )
    _subscriber_snapshot(db, 100, NOW - timedelta(days=1, hours=1))
    _subscriber_snapshot(db, 102, NOW - timedelta(days=1) + timedelta(hours=1))

    report = build_newsletter_topic_retention_report(
        db,
        lookback_issues=4,
        min_sends=1,
        now=NOW,
    )

    assert format_newsletter_topic_retention_json(
        report
    ) == format_newsletter_topic_retention_json(report)
    payload = json.loads(format_newsletter_topic_retention_json(report))
    assert payload["rows"][0]["topic"] == "delivery"
    text = format_newsletter_topic_retention_text(report)
    assert "Newsletter Topic Retention" in text
    assert "recommendation=grow_topic" in text

    with patch.object(
        newsletter_topic_retention,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        newsletter_topic_retention,
        "build_newsletter_topic_retention_report",
        wraps=lambda db, **kwargs: build_newsletter_topic_retention_report(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert (
            newsletter_topic_retention.main(
                ["--issues", "4", "--min-sends", "1", "--format", "json"]
            )
            == 0
        )

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["lookback_issues"] == 4
    assert cli_payload["rows"][0]["topic"] == "delivery"
