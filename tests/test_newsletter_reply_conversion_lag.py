"""Tests for newsletter reply conversion lag reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.newsletter_reply_conversion_lag import (
    build_newsletter_reply_conversion_lag_report,
    build_newsletter_reply_conversion_lag_report_from_db,
    format_newsletter_reply_conversion_lag_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_reply_conversion_lag.py"
spec = importlib.util.spec_from_file_location("newsletter_reply_conversion_lag_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(db, *, issue_id: str, subject: str, days_ago: int, source_ids: list[int]) -> int:
    cursor = db.conn.execute(
        """INSERT INTO newsletter_sends (issue_id, subject, source_content_ids, status, sent_at)
           VALUES (?, ?, ?, 'sent', ?)""",
        (issue_id, subject, json.dumps(source_ids), (NOW - timedelta(days=days_ago)).isoformat()),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _reply(db, *, content_id: int, detected_days_ago: float, author: str = "@reader") -> int:
    cursor = db.conn.execute(
        """INSERT INTO reply_queue
           (inbound_tweet_id, inbound_text, our_tweet_id, our_content_id, inbound_author_handle, detected_at)
           VALUES (?, 'reply', ?, ?, ?, ?)""",
        (
            f"inbound-{content_id}-{detected_days_ago}",
            f"tweet-{content_id}",
            content_id,
            author,
            (NOW - timedelta(days=detected_days_ago)).isoformat(),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_empty_input_returns_empty_summary():
    report = build_newsletter_reply_conversion_lag_report([], [], now=NOW)

    assert report["issues"] == []
    assert report["totals"]["record_count"] == 0
    assert report["totals"]["conversion_rate"] == 0.0
    assert report["empty_state"]["is_empty"] is True


def test_matches_replies_to_newsletter_source_content_ids():
    sends = [
        {
            "id": 1,
            "issue_id": "issue-1",
            "subject": "Launch notes",
            "source_content_ids": [101],
            "sent_at": (NOW - timedelta(days=4)).isoformat(),
        }
    ]
    replies = [
        {
            "id": 9,
            "our_content_id": 101,
            "inbound_author_handle": "@ada",
            "detected_at": (NOW - timedelta(days=2)).isoformat(),
        }
    ]

    report = build_newsletter_reply_conversion_lag_report(sends, replies, now=NOW)

    assert report["totals"]["converted_count"] == 1
    assert report["totals"]["week_1_count"] == 1
    issue = report["issues"][0]
    assert issue["lag_bucket"] == "week_1"
    assert issue["lag_days"] == 2.0
    assert issue["first_reply_id"] == 9
    assert issue["first_reply_author"] == "@ada"


def test_reports_no_conversion_with_age_information():
    sends = [
        {
            "id": 2,
            "issue_id": "issue-2",
            "subject": "No replies",
            "source_content_ids": [202],
            "sent_at": (NOW - timedelta(days=9)).isoformat(),
        }
    ]

    report = build_newsletter_reply_conversion_lag_report(sends, [], now=NOW)

    issue = report["issues"][0]
    assert issue["conversion_status"] == "no_conversion"
    assert issue["lag_bucket"] == "no_conversion"
    assert issue["age_days"] == 9.0
    assert issue["lag_days"] is None
    assert report["totals"]["no_conversion_count"] == 1


def test_stale_replies_and_risk_ordering():
    sends = [
        {
            "id": 1,
            "issue_id": "stale",
            "subject": "Late",
            "source_content_ids": [1],
            "sent_at": (NOW - timedelta(days=20)).isoformat(),
        },
        {
            "id": 2,
            "issue_id": "missing",
            "subject": "Missing",
            "source_content_ids": [2],
            "sent_at": (NOW - timedelta(days=8)).isoformat(),
        },
        {
            "id": 3,
            "issue_id": "fast",
            "subject": "Fast",
            "source_content_ids": [3],
            "sent_at": (NOW - timedelta(days=2)).isoformat(),
        },
    ]
    replies = [
        {"id": 1, "our_content_id": 1, "detected_at": (NOW - timedelta(days=1)).isoformat()},
        {"id": 3, "our_content_id": 3, "detected_at": (NOW - timedelta(days=1.5)).isoformat()},
    ]

    report = build_newsletter_reply_conversion_lag_report(sends, replies, now=NOW)

    assert [item["issue_id"] for item in report["issues"]] == ["missing", "stale", "fast"]
    assert report["issues"][1]["lag_bucket"] == "stale"
    assert "bucket" in format_newsletter_reply_conversion_lag_text(report)


def test_db_loader_and_cli_default_json_output(db, monkeypatch, capsys):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="post",
        eval_score=7,
        eval_feedback="ok",
    )
    send_id = _send(db, issue_id="db-issue", subject="DB", days_ago=4, source_ids=[content_id])
    reply_id = _reply(db, content_id=content_id, detected_days_ago=3)

    report = build_newsletter_reply_conversion_lag_report_from_db(db, now=NOW)
    assert report["issues"][0]["newsletter_send_id"] == send_id
    assert report["issues"][0]["first_reply_id"] == reply_id

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_newsletter_reply_conversion_lag_report_from_db",
        lambda db, **kwargs: build_newsletter_reply_conversion_lag_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "newsletter_reply_conversion_lag"
    assert payload["issues"][0]["first_reply_id"] == reply_id

    assert script.main(["--table"]) == 0
    assert "Newsletter Reply Conversion Lag" in capsys.readouterr().out
