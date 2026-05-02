"""Tests for reply follow-up reminder digest reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from engagement.reply_followup_digest import (
    build_reply_followup_digest_report,
    format_reply_followup_digest_json,
    format_reply_followup_digest_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_digest.py"
spec = importlib.util.spec_from_file_location("reply_followup_digest_script", SCRIPT_PATH)
reply_followup_digest_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_followup_digest_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, inbound_id: str, handle: str, *, platform: str = "x") -> int:
    return db.insert_reply_draft(
        inbound_tweet_id=inbound_id,
        inbound_author_handle=handle,
        inbound_author_id=f"{handle}-id",
        inbound_text=f"Original note from {handle}",
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
    reason: str = "High-value reply",
    created_at: str = "2026-04-29T12:00:00+00:00",
) -> int:
    reminder_id = db.insert_reply_followup_reminder(
        target_handle=handle,
        source_type="reply_queue",
        source_id=source_id,
        due_at=due_at,
        reason=reason,
    )
    db.conn.execute(
        """UPDATE reply_followup_reminders
           SET status = ?, created_at = ?, completed_at = CASE WHEN ? = 'done' THEN ? ELSE NULL END
           WHERE id = ?""",
        (status, created_at, status, NOW.isoformat(), reminder_id),
    )
    db.conn.commit()
    return reminder_id


def test_digest_separates_overdue_due_today_upcoming_and_completed(db):
    overdue_reply = _insert_reply(db, "overdue", "alice")
    today_reply = _insert_reply(db, "today", "bob")
    upcoming_reply = _insert_reply(db, "upcoming", "carol")
    far_reply = _insert_reply(db, "far", "dave")
    done_reply = _insert_reply(db, "done", "erin")

    _insert_reminder(db, handle="alice", source_id=overdue_reply, due_at="2026-04-30T10:00:00+00:00")
    _insert_reminder(db, handle="bob", source_id=today_reply, due_at="2026-05-01T18:00:00+00:00")
    _insert_reminder(db, handle="carol", source_id=upcoming_reply, due_at="2026-05-03T09:00:00+00:00")
    _insert_reminder(db, handle="dave", source_id=far_reply, due_at="2026-05-20T09:00:00+00:00")
    _insert_reminder(
        db,
        handle="erin",
        source_id=done_reply,
        due_at="2026-04-29T09:00:00+00:00",
        status="done",
    )

    report = build_reply_followup_digest_report(
        db,
        days_ahead=3,
        include_completed=True,
        now=NOW,
    )

    assert report["counts"] == {
        "overdue": 1,
        "due_today": 1,
        "upcoming": 1,
        "completed": 1,
        "total": 4,
    }
    assert [item["target_author"] for item in report["buckets"]["overdue"]] == ["alice"]
    assert [item["target_author"] for item in report["buckets"]["due_today"]] == ["bob"]
    assert [item["target_author"] for item in report["buckets"]["upcoming"]] == ["carol"]
    assert [item["target_author"] for item in report["buckets"]["completed"]] == ["erin"]
    assert "dave" not in [item["target_author"] for item in report["findings"]]


def test_findings_include_review_context_and_suggested_action(db):
    reply_id = _insert_reply(db, "ctx", "alice", platform="bluesky")
    reminder_id = _insert_reminder(
        db,
        handle="@fallback",
        source_id=reply_id,
        due_at="2026-04-30T10:00:00+00:00",
        reason="Warm relationship",
    )

    report = build_reply_followup_digest_report(db, now=NOW)
    finding = report["buckets"]["overdue"][0]

    assert finding["id"] == reminder_id
    assert finding["target_author"] == "alice"
    assert finding["platform"] == "bluesky"
    assert finding["original_reply_id"] == reply_id
    assert finding["due_at"] == "2026-04-30T10:00:00+00:00"
    assert finding["age_hours"] == 48.0
    assert finding["original_reply"]["inbound_id"] == "ctx"
    assert finding["original_reply"]["inbound_url"] == "https://example.test/ctx"
    assert finding["original_reply"]["inbound_text"] == "Original note from alice"
    assert "Send or dismiss" in finding["suggested_action"]


def test_completed_reminders_are_filtered_by_default(db):
    reply_id = _insert_reply(db, "done", "alice")
    _insert_reminder(
        db,
        handle="alice",
        source_id=reply_id,
        due_at="2026-04-30T10:00:00+00:00",
        status="done",
    )

    default_report = build_reply_followup_digest_report(db, include_completed=False, now=NOW)
    included_report = build_reply_followup_digest_report(db, include_completed=True, now=NOW)

    assert default_report["counts"]["total"] == 0
    assert included_report["counts"]["completed"] == 1


def test_platform_filter_uses_reply_queue_platform(db):
    x_reply = _insert_reply(db, "x-reply", "alice", platform="x")
    bluesky_reply = _insert_reply(db, "bsky-reply", "bob", platform="bluesky")
    _insert_reminder(db, handle="alice", source_id=x_reply, due_at="2026-04-30T10:00:00+00:00")
    _insert_reminder(db, handle="bob", source_id=bluesky_reply, due_at="2026-05-01T10:00:00+00:00")

    report = build_reply_followup_digest_report(db, platform="bluesky", now=NOW)

    assert report["counts"]["total"] == 1
    assert report["findings"][0]["target_author"] == "bob"
    assert report["findings"][0]["platform"] == "bluesky"


def test_missing_reply_queue_row_still_reports_reminder(db):
    _insert_reminder(
        db,
        handle="missing-author",
        source_id=999,
        due_at="2026-04-30T10:00:00+00:00",
    )

    report = build_reply_followup_digest_report(db, now=NOW)
    finding = report["buckets"]["overdue"][0]

    assert finding["target_author"] == "missing-author"
    assert finding["platform"] == "unknown"
    assert finding["original_reply_id"] == 999
    assert finding["original_reply"] is None
    assert "checking the source context" in finding["suggested_action"]


def test_json_and_text_output_are_deterministic(db):
    later_reply = _insert_reply(db, "later", "zoe")
    earlier_reply = _insert_reply(db, "earlier", "amy")
    _insert_reminder(db, handle="zoe", source_id=later_reply, due_at="2026-05-01T18:00:00+00:00")
    _insert_reminder(db, handle="amy", source_id=earlier_reply, due_at="2026-04-30T10:00:00+00:00")

    report = build_reply_followup_digest_report(db, now=NOW)
    payload = json.loads(format_reply_followup_digest_json(report))
    text = format_reply_followup_digest_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert [item["target_author"] for item in payload["findings"]] == ["amy", "zoe"]
    assert "Reply Follow-up Digest" in text
    assert "Overdue:" in text
    assert "Due Today:" in text


def test_missing_reminder_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_followup_digest_report(conn, now=NOW)

    assert report["counts"]["total"] == 0
    assert report["missing_tables"] == ["reply_followup_reminders"]


def test_invalid_arguments_raise_value_error(db):
    with pytest.raises(ValueError, match="days_ahead must be non-negative"):
        build_reply_followup_digest_report(db, days_ahead=-1, now=NOW)
    with pytest.raises(ValueError, match="platform must not be blank"):
        build_reply_followup_digest_report(db, platform=" ", now=NOW)


def test_cli_supports_json_filters_and_completed_flag(db, monkeypatch, capsys):
    reply_id = _insert_reply(db, "cli", "alice", platform="x")
    _insert_reminder(
        db,
        handle="alice",
        source_id=reply_id,
        due_at="2026-04-30T10:00:00+00:00",
        status="done",
    )
    monkeypatch.setattr(
        reply_followup_digest_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_followup_digest_script,
        "build_reply_followup_digest_report",
        lambda db, **kwargs: build_reply_followup_digest_report(db, now=NOW, **kwargs),
    )

    exit_code = reply_followup_digest_script.main(
        [
            "--days-ahead",
            "2",
            "--include-completed",
            "--platform",
            "x",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["days_ahead"] == 2
    assert payload["filters"]["include_completed"] is True
    assert payload["counts"]["completed"] == 1
