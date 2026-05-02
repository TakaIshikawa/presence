"""Tests for reply follow-up due-window reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.reply_followup_due_windows import (
    build_reply_followup_due_windows_report,
    format_reply_followup_due_windows_json,
    format_reply_followup_due_windows_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_due_windows.py"
spec = importlib.util.spec_from_file_location("reply_followup_due_windows_script", SCRIPT_PATH)
reply_followup_due_windows_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_followup_due_windows_script)


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


def test_due_soon_and_overdue_are_classified_from_pending_due_at(db):
    overdue_reply = _insert_reply(db, "overdue", "alice")
    soon_reply = _insert_reply(db, "soon", "bob")
    later_reply = _insert_reply(db, "later", "cam")
    overdue_id = _insert_reminder(
        db,
        handle="alice",
        source_id=overdue_reply,
        due_at="2026-05-02T08:00:00+00:00",
    )
    due_soon_id = _insert_reminder(
        db,
        handle="bob",
        source_id=soon_reply,
        due_at="2026-05-02T18:00:00+00:00",
    )
    _insert_reminder(
        db,
        handle="cam",
        source_id=later_reply,
        due_at="2026-05-04T12:00:00+00:00",
    )

    report = build_reply_followup_due_windows_report(db, horizon_hours=24, now=NOW)

    assert report["artifact_type"] == "reply_followup_due_windows"
    assert report["generated_at"] == "2026-05-02T12:00:00+00:00"
    assert report["horizon_hours"] == 24
    assert report["target_handle"] is None
    assert report["totals"] == {
        "blocked_source": 0,
        "due_soon": 1,
        "missing_target": 0,
        "overdue": 1,
        "total": 2,
    }
    assert [item["id"] for item in report["buckets"]["overdue"]] == [overdue_id]
    assert [item["id"] for item in report["buckets"]["due_soon"]] == [due_soon_id]
    assert report["findings"][0]["bucket"] == "overdue"
    assert report["findings"][1]["bucket"] == "due_soon"


def test_done_and_cancelled_reminder_statuses_are_excluded():
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
    conn.executemany(
        """INSERT INTO reply_followup_reminders
           (id, target_handle, source_type, source_id, due_at, status, reason)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (1, "alice", "reply_queue", 1, "2026-05-02T08:00:00+00:00", "done", "done"),
            (2, "bob", "reply_queue", 2, "2026-05-02T09:00:00+00:00", "cancelled", "cancelled"),
            (3, "cam", "reply_queue", 3, "2026-05-02T10:00:00+00:00", "pending", "pending"),
        ],
    )

    report = build_reply_followup_due_windows_report(conn, now=NOW)

    assert [item["id"] for item in report["findings"]] == [3]
    conn.close()


def test_missing_target_and_blocked_source_surface_recommended_actions(db):
    missing_reply = _insert_reply(db, "missing", "")
    blocked_reply = _insert_reply(db, "blocked", "casey", status="dismissed")
    missing_id = _insert_reminder(
        db,
        handle="",
        source_id=missing_reply,
        due_at="2026-05-02T14:00:00+00:00",
    )
    blocked_id = _insert_reminder(
        db,
        handle="casey",
        source_id=blocked_reply,
        due_at="2026-05-02T15:00:00+00:00",
    )

    report = build_reply_followup_due_windows_report(db, now=NOW)

    missing = report["buckets"]["missing_target"][0]
    blocked = report["buckets"]["blocked_source"][0]
    assert missing["id"] == missing_id
    assert missing["target_handle"] is None
    assert missing["recommended_action"] == (
        "Add a target_handle before sending or dismissing this follow-up."
    )
    assert blocked["id"] == blocked_id
    assert blocked["source_status"] == "dismissed"
    assert "no longer actionable" in blocked["recommended_action"]


def test_target_handle_filter_uses_joined_reply_author_when_available(db):
    reply_id = _insert_reply(db, "joined", "alice")
    _insert_reminder(
        db,
        handle="fallback",
        source_id=reply_id,
        due_at="2026-05-02T14:00:00+00:00",
    )

    report = build_reply_followup_due_windows_report(
        db,
        target_handle="@ALICE",
        now=NOW,
    )

    assert report["target_handle"] == "alice"
    assert report["totals"]["due_soon"] == 1
    assert report["findings"][0]["target_handle"] == "alice"
    assert report["findings"][0]["stored_target_handle"] == "fallback"
    assert report["findings"][0]["source_context"]["author_handle"] == "alice"


def test_report_still_works_without_reply_queue_join():
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
           VALUES (1, 'alice', 'reply_queue', 10, '2026-05-02T13:00:00+00:00',
                   'pending', 'No join')"""
    )

    report = build_reply_followup_due_windows_report(conn, now=NOW)

    finding = report["findings"][0]
    assert finding["bucket"] == "due_soon"
    assert finding["target_handle"] == "alice"
    assert finding["source_status"] is None
    assert finding["source_context"] is None
    conn.close()


def test_json_text_and_cli_formatting_are_stable(db, monkeypatch, capsys):
    reply_id = _insert_reply(db, "cli", "alice")
    _insert_reminder(
        db,
        handle="alice",
        source_id=reply_id,
        due_at="2026-05-02T13:00:00+00:00",
        reason="CLI check",
    )

    report = build_reply_followup_due_windows_report(db, horizon_hours=6, now=NOW)
    payload = json.loads(format_reply_followup_due_windows_json(report))
    text = format_reply_followup_due_windows_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["totals"]["due_soon"] == 1
    assert "Reply Follow-up Due Windows" in text
    assert "Horizon: 6h target=all" in text
    assert "recommended_action=" in text

    monkeypatch.setattr(
        reply_followup_due_windows_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_followup_due_windows_script,
        "build_reply_followup_due_windows_report",
        lambda db, **kwargs: build_reply_followup_due_windows_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert reply_followup_due_windows_script.main(
        ["--horizon-hours", "6", "--target-handle", "alice", "--format", "json"]
    ) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["horizon_hours"] == 6
    assert cli_payload["target_handle"] == "alice"
    assert cli_payload["totals"]["due_soon"] == 1

    assert reply_followup_due_windows_script.main(["--horizon-hours", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_missing_schema_and_invalid_horizon_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_followup_due_windows_report(conn, now=NOW)

    assert report["missing_tables"] == ["reply_followup_reminders"]
    assert report["totals"]["total"] == 0
    assert "Missing tables: reply_followup_reminders" in (
        format_reply_followup_due_windows_text(report)
    )
    with pytest.raises(ValueError, match="horizon_hours must be positive"):
        build_reply_followup_due_windows_report(conn, horizon_hours=0, now=NOW)
    conn.close()
