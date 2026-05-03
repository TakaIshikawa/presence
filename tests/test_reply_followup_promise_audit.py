"""Tests for reply follow-up promise auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.reply_followup_promise_audit import (
    build_reply_followup_promise_audit,
    detected_promise_phrases,
    format_reply_followup_promise_audit_json,
    format_reply_followup_promise_audit_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_promise_audit.py"
spec = importlib.util.spec_from_file_location("reply_followup_promise_audit_script", SCRIPT_PATH)
reply_followup_promise_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_followup_promise_audit_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, inbound_id: str, draft_text: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_author_id="author-a",
        inbound_text="Can you help?",
        our_tweet_id="our-1",
        our_content_id=123,
        our_post_text="Original post",
        draft_text=draft_text,
        platform="x",
        inbound_url=f"https://x.com/alice/status/{inbound_id}",
        status="pending",
    )
    defaults.update(kwargs)
    reply_id = db.insert_reply_draft(**defaults)
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        ("2026-05-03T10:00:00+00:00", reply_id),
    )
    db.conn.commit()
    return reply_id


def test_pending_reply_drafts_with_followup_language_are_blocking_findings(db):
    first = _insert_reply(db, "follow-up", "I will follow up tomorrow with the details.")
    second = _insert_reply(db, "send", "I'll send a concrete example next week.")
    _insert_reply(db, "plain", "Thanks, that makes sense.")

    report = build_reply_followup_promise_audit(db, now=NOW)
    payload = json.loads(format_reply_followup_promise_audit_json(report))

    assert report.ok is False
    assert report.blocking_issue_count == 2
    assert payload["finding_count"] == 2
    assert payload["promised_count"] == 2
    assert [finding["reply_queue_id"] for finding in payload["findings"]] == [first, second]
    assert payload["findings"][0]["detected_promise_phrases"] == [
        "will follow up",
        "tomorrow",
    ]
    assert payload["findings"][1]["detected_promise_phrases"] == [
        "next week",
        "I'll send",
    ]
    assert payload["findings"][0]["due_metadata_status"] == "missing"
    assert payload["findings"][0]["severity"] == "high"
    assert payload["findings"][0]["inbound_author_handle"] == "alice"


def test_metadata_due_at_and_action_reference_suppress_findings(db):
    _insert_reply(
        db,
        "due-at",
        "I will follow up tomorrow.",
        platform_metadata=json.dumps({"followup_at": "2026-05-04T09:00:00+00:00"}),
    )
    _insert_reply(
        db,
        "action",
        "I can circle back next week.",
        relationship_context=json.dumps({"followup": {"action_reference": "reply_followup:42"}}),
    )
    missing = _insert_reply(db, "missing", "I'll send the patch tomorrow.")

    report = build_reply_followup_promise_audit(db, now=NOW)

    assert [finding.reply_queue_id for finding in report.findings] == [missing]
    assert report.promised_count == 3


def test_followup_reminder_record_suppresses_matching_reply(db):
    tracked = _insert_reply(db, "tracked", "I will follow up next week.")
    missing = _insert_reply(db, "untracked", "I will follow up tomorrow.")
    reminder_id = db.insert_reply_followup_reminder(
        target_handle="alice",
        source_type="reply_queue",
        source_id=tracked,
        due_at="2026-05-08T09:00:00+00:00",
        reason="Promised in reply draft",
    )
    assert reminder_id is not None

    report = build_reply_followup_promise_audit(db, now=NOW)

    assert [finding.reply_queue_id for finding in report.findings] == [missing]


def test_fixture_rows_support_filters_and_followup_records():
    rows = [
        {
            "id": 1,
            "status": "pending",
            "platform": "x",
            "draft_text": "I will follow up tomorrow.",
            "detected_at": "2026-05-03T10:00:00+00:00",
        },
        {
            "id": 2,
            "status": "pending",
            "platform": "bluesky",
            "draft_text": "I will follow up next week.",
            "detected_at": "2026-05-03T11:00:00+00:00",
        },
        {
            "id": 3,
            "status": "posted",
            "platform": "x",
            "draft_text": "I will follow up tomorrow.",
            "detected_at": "2026-05-03T11:30:00+00:00",
        },
    ]
    followups = [{"source_type": "reply_queue", "source_id": 1, "due_at": "2026-05-04"}]

    report = build_reply_followup_promise_audit(
        reply_records=rows,
        followup_records=followups,
        platform="x",
        now=NOW,
    )
    all_report = build_reply_followup_promise_audit(
        reply_records=rows,
        followup_records=followups,
        now=NOW,
    )

    assert report.audited_count == 1
    assert report.finding_count == 0
    assert [finding.reply_queue_id for finding in all_report.findings] == [2]


def test_json_text_and_cli_support_formats_db_platform_limit_and_exit_codes(db, monkeypatch, capsys):
    _insert_reply(db, "cli-x", "I will follow up tomorrow.")
    _insert_reply(
        db,
        "cli-bsky",
        "I can circle back next week.",
        platform="bluesky",
        inbound_url="https://bsky.app/profile/alice/post/cli-bsky",
    )
    monkeypatch.setattr(
        reply_followup_promise_audit_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = reply_followup_promise_audit_script.main(
        ["--format", "json", "--platform", "bluesky", "--days", "3", "--limit", "1"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"] == {
        "days": 3,
        "limit": 1,
        "platform": ["bluesky"],
        "status": "pending",
    }
    assert payload["finding_count"] == 1
    assert payload["findings"][0]["platform"] == "bluesky"

    text_exit = reply_followup_promise_audit_script.main(["--platform", "x"])
    text = capsys.readouterr().out
    assert text_exit == 1
    assert "Reply Follow-up Promise Audit" in text

    report = build_reply_followup_promise_audit(db, now=NOW)
    assert list(json.loads(format_reply_followup_promise_audit_json(report))) == sorted(
        json.loads(format_reply_followup_promise_audit_json(report))
    )
    assert "findings=2" in format_reply_followup_promise_audit_text(report)

    invalid = reply_followup_promise_audit_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "value must be positive" in captured.err


def test_db_path_option_and_missing_schema_are_handled(tmp_path):
    db_path = tmp_path / "replies.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            platform TEXT,
            inbound_author_handle TEXT,
            inbound_author_id TEXT,
            inbound_tweet_id TEXT,
            inbound_url TEXT,
            draft_text TEXT,
            detected_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO reply_queue
           (id, status, platform, inbound_author_handle, inbound_author_id,
            inbound_tweet_id, inbound_url, draft_text, detected_at)
           VALUES (1, 'pending', 'x', 'alice', 'author-a', 'in-1',
                   'https://example.test/in-1', 'I will follow up tomorrow.',
                   '2026-05-03T10:00:00+00:00')"""
    )
    conn.commit()
    conn.close()

    assert reply_followup_promise_audit_script.main(
        ["--db", str(db_path), "--format", "json"]
    ) == 1

    missing = build_reply_followup_promise_audit(sqlite3.connect(":memory:"), now=NOW)
    assert missing.ok is True
    assert missing.missing_tables == ("reply_queue",)


def test_phrase_detection_and_invalid_arguments():
    assert detected_promise_phrases("I will follow up tomorrow and circle back next week.") == (
        "will follow up",
        "circle back",
        "tomorrow",
        "next week",
    )
    with pytest.raises(ValueError, match="days must be positive"):
        build_reply_followup_promise_audit([], days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_reply_followup_promise_audit([], limit=0, now=NOW)
