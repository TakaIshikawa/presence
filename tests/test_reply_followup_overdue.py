"""Tests for overdue reply follow-up escalation reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from engagement.reply_followup_overdue import (
    build_reply_followup_overdue_report,
    format_reply_followup_overdue_json,
    format_reply_followup_overdue_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_overdue.py"
spec = importlib.util.spec_from_file_location("reply_followup_overdue_script", SCRIPT_PATH)
reply_followup_overdue_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_followup_overdue_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, inbound_id: str, handle: str, *, platform: str = "x") -> int:
    return db.insert_reply_draft(
        inbound_tweet_id=inbound_id,
        inbound_author_handle=handle,
        inbound_author_id=f"{handle}-id",
        inbound_text=f"Question from {handle}",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text=f"Reply to {handle}",
        platform=platform,
        status="posted",
        inbound_url=f"https://example.test/{inbound_id}",
    )


def _insert_reminder(
    db,
    *,
    handle: str,
    source_id: int,
    due_at: str,
    status: str = "pending",
    reason: str = "Warm thread needs follow-up",
) -> int:
    reminder_id = db.insert_reply_followup_reminder(
        target_handle=handle,
        source_type="reply_queue",
        source_id=source_id,
        due_at=due_at,
        reason=reason,
    )
    assert reminder_id is not None
    db.conn.execute(
        "UPDATE reply_followup_reminders SET status = ? WHERE id = ?",
        (status, reminder_id),
    )
    db.conn.commit()
    return reminder_id


def test_empty_data_returns_stable_empty_report(db):
    report = build_reply_followup_overdue_report(db, now=NOW)

    assert report["artifact_type"] == "reply_followup_overdue"
    assert report["totals"] == {
        "total": 0,
        "by_severity": {"urgent": 0, "stale": 0, "watch": 0},
        "target_handles": 0,
    }
    assert report["overdue_buckets"] == [
        {"severity": "urgent", "count": 0, "reminders": []},
        {"severity": "stale", "count": 0, "reminders": []},
        {"severity": "watch", "count": 0, "reminders": []},
    ]
    assert report["representative_reminders"] == []


def test_missing_tables_and_columns_return_empty_reports():
    missing_table = sqlite3.connect(":memory:")
    missing_table.row_factory = sqlite3.Row

    report = build_reply_followup_overdue_report(missing_table, now=NOW)

    assert report["missing_tables"] == ["reply_followup_reminders"]
    assert report["reminders"] == []

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute(
        """CREATE TABLE reply_followup_reminders (
            id INTEGER PRIMARY KEY,
            target_handle TEXT,
            due_at TEXT
        )"""
    )

    report = build_reply_followup_overdue_report(partial, now=NOW)

    assert report["missing_columns"]["reply_followup_reminders"] == [
        "reason",
        "source_id",
        "source_type",
        "status",
    ]
    assert report["reminders"] == []


def test_overdue_pending_reminders_are_grouped_and_sorted_by_severity_age_and_id(db):
    urgent_old = _insert_reply(db, "urgent-old", "zoe")
    urgent_new = _insert_reply(db, "urgent-new", "amy")
    stale = _insert_reply(db, "stale", "bob")
    watch = _insert_reply(db, "watch", "cam")
    future = _insert_reply(db, "future", "dee")
    done = _insert_reply(db, "done", "eve")

    urgent_old_id = _insert_reminder(
        db,
        handle="zoe",
        source_id=urgent_old,
        due_at="2026-05-01T05:00:00+00:00",
    )
    urgent_new_id = _insert_reminder(
        db,
        handle="amy",
        source_id=urgent_new,
        due_at="2026-05-01T11:00:00+00:00",
    )
    stale_id = _insert_reminder(
        db,
        handle="bob",
        source_id=stale,
        due_at="2026-05-02T00:00:00+00:00",
    )
    watch_id = _insert_reminder(
        db,
        handle="cam",
        source_id=watch,
        due_at="2026-05-02T08:00:00+00:00",
    )
    _insert_reminder(db, handle="dee", source_id=future, due_at="2026-05-03T08:00:00+00:00")
    _insert_reminder(
        db,
        handle="eve",
        source_id=done,
        due_at="2026-05-01T08:00:00+00:00",
        status="done",
    )

    report = build_reply_followup_overdue_report(
        db,
        high_priority_hours=24,
        limit=3,
        now=NOW,
    )

    assert [item["id"] for item in report["reminders"]] == [
        urgent_old_id,
        urgent_new_id,
        stale_id,
        watch_id,
    ]
    assert [item["severity"] for item in report["reminders"]] == [
        "urgent",
        "urgent",
        "stale",
        "watch",
    ]
    assert [item["id"] for item in report["representative_reminders"]] == [
        urgent_old_id,
        urgent_new_id,
        stale_id,
    ]
    assert report["totals"]["by_severity"] == {"urgent": 2, "stale": 1, "watch": 1}


def test_json_and_text_formatting_include_required_operator_fields(db):
    reply_id = _insert_reply(db, "ctx", "alice", platform="bluesky")
    _insert_reminder(
        db,
        handle="@fallback",
        source_id=reply_id,
        due_at="2026-05-01T06:00:00+00:00",
        reason="High-signal reply",
    )

    report = build_reply_followup_overdue_report(db, now=NOW)
    payload = json.loads(format_reply_followup_overdue_json(report))
    text = format_reply_followup_overdue_text(report)

    reminder = payload["reminders"][0]
    assert list(payload.keys()) == sorted(payload.keys())
    assert reminder["target_handle"] == "alice"
    assert reminder["due_at"] == "2026-05-01T06:00:00+00:00"
    assert reminder["source_type"] == "reply_queue"
    assert reminder["source_id"] == reply_id
    assert reminder["reason"] == "High-signal reply"
    assert "Escalate" in reminder["recommendation"]
    assert "Reply Follow-up Overdue Escalations" in text
    assert "@alice" in text
    assert "due=2026-05-01T06:00:00+00:00" in text
    assert "source=reply_queue:" in text
    assert "recommendation=" in text


def test_joined_reply_context_is_included_when_available(db):
    reply_id = _insert_reply(db, "joined", "alice", platform="mastodon")
    _insert_reminder(
        db,
        handle="fallback",
        source_id=reply_id,
        due_at="2026-05-01T06:00:00+00:00",
    )

    report = build_reply_followup_overdue_report(db, now=NOW)
    reminder = report["reminders"][0]

    assert reminder["target_handle"] == "alice"
    assert reminder["platform"] == "mastodon"
    assert reminder["reply_context"] == {
        "reply_queue_id": reply_id,
        "platform": "mastodon",
        "inbound_id": "joined",
        "inbound_url": "https://example.test/joined",
        "author_handle": "alice",
        "author_id": "alice-id",
        "inbound_text": "Question from alice",
        "draft_text": "Reply to alice",
        "status": "posted",
    }


def test_invalid_builder_and_cli_numeric_arguments(db, monkeypatch, capsys):
    with pytest.raises(ValueError, match="days must be positive"):
        build_reply_followup_overdue_report(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="high_priority_hours must be positive"):
        build_reply_followup_overdue_report(db, high_priority_hours=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_reply_followup_overdue_report(db, limit=0, now=NOW)

    assert reply_followup_overdue_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    monkeypatch.setattr(
        reply_followup_overdue_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_followup_overdue_script,
        "build_reply_followup_overdue_report",
        lambda db, **kwargs: build_reply_followup_overdue_report(db, now=NOW, **kwargs),
    )

    reply_id = _insert_reply(db, "cli", "alice")
    _insert_reminder(db, handle="alice", source_id=reply_id, due_at="2026-05-01T06:00:00+00:00")

    assert reply_followup_overdue_script.main(
        ["--days", "3", "--high-priority-hours", "6", "--limit", "1", "--format", "json"]
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days"] == 3
    assert payload["filters"]["high_priority_hours"] == 6
    assert payload["filters"]["limit"] == 1
