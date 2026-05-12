"""Tests for reply follow-up due reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.reply_followup_due import (
    build_reply_followup_due_report,
    format_reply_followup_due_json,
    format_reply_followup_due_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_due.py"
spec = importlib.util.spec_from_file_location("reply_followup_due_script", SCRIPT_PATH)
reply_followup_due_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_followup_due_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, inbound_id: str, handle: str, *, status: str = "posted") -> int:
    return db.insert_reply_draft(
        inbound_tweet_id=inbound_id,
        inbound_author_handle=handle,
        inbound_author_id=f"{handle}-id",
        inbound_text=f"Question from {handle}",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text=f"Reply to {handle}",
        platform="bluesky",
        status=status,
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


def test_pending_reminders_are_bucketed_relative_to_current_time(db):
    overdue_id = _insert_reminder(
        db,
        handle="alice",
        source_id=_insert_reply(db, "overdue", "alice"),
        due_at="2026-05-02T08:00:00+00:00",
    )
    today_id = _insert_reminder(
        db,
        handle="bob",
        source_id=_insert_reply(db, "today", "bob"),
        due_at="2026-05-02T18:00:00+00:00",
    )
    upcoming_id = _insert_reminder(
        db,
        handle="cam",
        source_id=_insert_reply(db, "upcoming", "cam"),
        due_at="2026-05-04T09:00:00+00:00",
    )
    _insert_reminder(
        db,
        handle="dee",
        source_id=_insert_reply(db, "later", "dee"),
        due_at="2026-05-12T09:00:00+00:00",
    )

    report = build_reply_followup_due_report(db, days_ahead=3, now=NOW)

    assert report["artifact_type"] == "reply_followup_due"
    assert report["generated_at"] == "2026-05-02T12:00:00+00:00"
    assert report["filters"]["window_end"] == "2026-05-05T12:00:00+00:00"
    assert report["totals"]["by_status"] == {
        "dismissed": 0,
        "done": 0,
        "due_today": 1,
        "overdue": 1,
        "upcoming": 1,
    }
    assert [item["id"] for item in report["items"]] == [overdue_id, today_id, upcoming_id]
    assert report["representative_ids"] == [overdue_id, today_id, upcoming_id]


def test_done_and_dismissed_statuses_are_counted(db):
    done_id = _insert_reminder(
        db,
        handle="alice",
        source_id=_insert_reply(db, "done", "alice"),
        due_at="2026-05-01T08:00:00+00:00",
        status="done",
    )
    dismissed_id = _insert_reminder(
        db,
        handle="bob",
        source_id=_insert_reply(db, "dismissed", "bob"),
        due_at="2026-05-01T09:00:00+00:00",
        status="dismissed",
    )

    report = build_reply_followup_due_report(db, now=NOW)

    assert [item["id"] for item in report["items"]] == [done_id, dismissed_id]
    assert report["totals"]["by_status"]["done"] == 1
    assert report["totals"]["by_status"]["dismissed"] == 1
    assert report["representative_ids"] == []


def test_reply_source_metadata_uses_joined_author_when_available(db):
    reply_id = _insert_reply(db, "joined", "alice")
    _insert_reminder(
        db,
        handle="fallback",
        source_id=reply_id,
        due_at="2026-05-02T13:00:00+00:00",
    )

    report = build_reply_followup_due_report(db, now=NOW)
    item = report["items"][0]

    assert item["target_handle"] == "alice"
    assert item["stored_target_handle"] == "fallback"
    assert item["source_context"] == {
        "reply_queue_id": reply_id,
        "author_handle": "alice",
        "author_id": "alice-id",
        "inbound_id": "joined",
        "inbound_url": "https://example.test/joined",
        "status": "posted",
    }
    assert item["platform"] == "bluesky"


def test_missing_optional_source_table_is_reported_without_failing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_followup_reminders (
            id INTEGER PRIMARY KEY,
            target_handle TEXT,
            source_type TEXT,
            source_id INTEGER,
            due_at TEXT,
            status TEXT,
            reason TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO reply_followup_reminders
           (id, target_handle, source_type, source_id, due_at, status, reason)
           VALUES (1, 'alice', 'reply_queue', 42, '2026-05-02T14:00:00+00:00',
                   'pending', 'No source table')"""
    )

    report = build_reply_followup_due_report(conn, now=NOW)

    assert report["missing_source_tables"] == ["reply_queue"]
    assert report["items"][0]["target_handle"] == "alice"
    assert report["items"][0]["source_context"] is None
    assert "Missing source tables: reply_queue" in format_reply_followup_due_text(report)
    conn.close()


def test_json_text_and_cli_formatting_are_stable(db, monkeypatch, capsys):
    _insert_reminder(
        db,
        handle="alice",
        source_id=_insert_reply(db, "cli", "alice"),
        due_at="2026-05-02T13:00:00+00:00",
        reason="CLI check",
    )

    report = build_reply_followup_due_report(db, days_ahead=2, limit=5, now=NOW)
    payload = json.loads(format_reply_followup_due_json(report))
    text = format_reply_followup_due_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["totals"]["by_status"]["due_today"] == 1
    assert "Reply Follow-up Due" in text
    assert "Status counts: overdue=0 due_today=1 upcoming=0 done=0 dismissed=0 total=1" in text
    assert "Actionable reminders:" in text

    monkeypatch.setattr(
        reply_followup_due_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_followup_due_script,
        "build_reply_followup_due_report",
        lambda db, **kwargs: build_reply_followup_due_report(db, now=NOW, **kwargs),
    )

    assert reply_followup_due_script.main(["--days-ahead", "2", "--limit", "5", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["days_ahead"] == 2
    assert cli_payload["filters"]["limit"] == 5

    assert reply_followup_due_script.main(["--days-ahead", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err


def test_missing_required_schema_and_invalid_builder_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_followup_due_report(conn, now=NOW)
    assert report["missing_tables"] == ["reply_followup_reminders"]
    assert report["items"] == []

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE reply_followup_reminders (id INTEGER PRIMARY KEY, due_at TEXT)")
    report = build_reply_followup_due_report(partial, now=NOW)
    assert report["missing_columns"]["reply_followup_reminders"] == [
        "reason",
        "source_id",
        "source_type",
        "status",
        "target_handle",
    ]

    with pytest.raises(ValueError, match="days_ahead must be non-negative"):
        build_reply_followup_due_report(conn, days_ahead=-1, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_reply_followup_due_report(conn, limit=0, now=NOW)
    conn.close()
    partial.close()
