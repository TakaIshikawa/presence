"""Tests for newsletter send-time recommendations."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.newsletter_send_time import (
    NewsletterSendTimeRecommender,
    score_send_time_window,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "recommend_newsletter_send_time.py"
)
spec = importlib.util.spec_from_file_location(
    "recommend_newsletter_send_time", SCRIPT_PATH
)
recommend_newsletter_send_time = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recommend_newsletter_send_time)

BASE_TIME = datetime(2026, 4, 27, 14, 0, tzinfo=timezone.utc)  # Monday


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _record_send(
    db,
    issue_id: str,
    *,
    sent_at: datetime,
    subscribers: int = 100,
    opens: int | None = 40,
    clicks: int | None = 4,
    unsubscribes: int = 0,
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Subject {issue_id}",
        content_ids=[],
        subscriber_count=subscribers,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at.isoformat(), send_id),
    )
    db.conn.commit()
    if opens is not None and clicks is not None:
        db.insert_newsletter_engagement(
            newsletter_send_id=send_id,
            issue_id=issue_id,
            opens=1,
            clicks=1,
            unsubscribes=0,
            fetched_at=(sent_at + timedelta(minutes=5)).isoformat(),
        )
        db.insert_newsletter_engagement(
            newsletter_send_id=send_id,
            issue_id=issue_id,
            opens=opens,
            clicks=clicks,
            unsubscribes=unsubscribes,
            fetched_at=(sent_at + timedelta(minutes=10)).isoformat(),
        )
    return send_id


def test_score_send_time_window_documents_formula():
    assert score_send_time_window(0.4, 0.08, 0.02) == 62.0


def test_recommendations_rank_by_latest_snapshot_score(db):
    for index in range(3):
        _record_send(
            db,
            f"monday-{index}",
            sent_at=BASE_TIME + timedelta(minutes=index),
            opens=45,
            clicks=8,
            unsubscribes=0,
        )
    for index in range(3):
        _record_send(
            db,
            f"tuesday-{index}",
            sent_at=BASE_TIME + timedelta(days=1, hours=1, minutes=index),
            opens=60,
            clicks=1,
            unsubscribes=4,
        )

    report = NewsletterSendTimeRecommender(db).recommend(
        days=90, min_sample=2, limit=5
    )

    assert report.score_formula == "open_rate*100 + click_rate*300 - unsubscribe_rate*100"
    assert report.recommended_count == 2
    assert report.recommendations[0].weekday_name == "Monday"
    assert report.recommendations[0].hour == 14
    assert report.recommendations[0].open_rate == 0.45
    assert report.recommendations[0].click_rate == 0.08
    assert report.recommendations[0].score == 69.0
    assert report.recommendations[1].weekday_name == "Tuesday"


def test_report_keeps_insufficient_sample_windows(db):
    _record_send(
        db,
        "single",
        sent_at=BASE_TIME,
        opens=80,
        clicks=10,
    )

    report = NewsletterSendTimeRecommender(db).recommend(
        days=90, min_sample=2, limit=5
    )

    assert report.recommended_count == 0
    assert report.windows[0].weekday_name == "Monday"
    assert report.windows[0].metric_sends == 1
    assert report.windows[0].recommendation == "insufficient_sample"
    assert "minimum sample of 2" in report.notes[0]


def test_report_groups_missing_engagement_without_failing(db):
    _record_send(db, "with-metrics", sent_at=BASE_TIME, opens=40, clicks=4)
    _record_send(db, "missing-metrics", sent_at=BASE_TIME, opens=None, clicks=None)

    report = NewsletterSendTimeRecommender(db).recommend(
        days=90, min_sample=1, limit=5
    )

    window = report.windows[0]
    assert window.sends == 2
    assert window.metric_sends == 1
    assert window.missing_engagement_sends == 1
    assert report.missing_engagement_sends == 1
    assert report.recommendations[0].score == 52.0


def test_report_handles_missing_engagement_columns_gracefully():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            subject TEXT,
            subscriber_count INTEGER,
            sent_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE newsletter_engagement (
            newsletter_send_id INTEGER,
            issue_id TEXT,
            opens INTEGER,
            fetched_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, subscriber_count, sent_at)
           VALUES (1, 'issue-1', 'Partial metrics', 100, ?)""",
        (BASE_TIME.isoformat(),),
    )
    conn.commit()

    report = NewsletterSendTimeRecommender(SimpleNamespace(conn=conn)).recommend(
        days=90, min_sample=1
    )

    assert report.total_sends == 1
    assert report.recommended_count == 0
    assert report.windows[0].missing_engagement_sends == 1
    assert "missing columns: clicks, unsubscribes" in report.notes[0]


def test_cli_formats_json_and_text(db, capsys):
    _record_send(
        db,
        "cli-1",
        sent_at=BASE_TIME,
        opens=50,
        clicks=6,
    )

    with patch.object(
        recommend_newsletter_send_time,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = recommend_newsletter_send_time.main(
            ["--days", "90", "--min-sample", "1", "--format", "json", "--limit", "1"]
        )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["period_days"] == 90
    assert payload["recommendations"][0]["weekday_name"] == "Monday"
    assert payload["recommendations"][0]["hour"] == 14

    with patch.object(
        recommend_newsletter_send_time,
        "script_context",
        return_value=_script_context(db),
    ):
        recommend_newsletter_send_time.main(["--days", "90", "--min-sample", "1"])

    text = capsys.readouterr().out
    assert "Newsletter Send-Time Recommendations (last 90 days)" in text
    assert "1. Monday 14:00" in text
    assert "Score: open_rate*100 + click_rate*300 - unsubscribe_rate*100" in text
