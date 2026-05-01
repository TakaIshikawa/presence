"""Tests for reply stale-context detection."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.reply_stale_context import (  # noqa: E402
    build_reply_stale_context_report,
    format_reply_stale_context_text,
    inspect_reply_stale_context,
)
from reply_stale_context import main  # noqa: E402


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def _insert_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks for sharing this.",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_age_threshold_marks_old_pending_draft_stale(db):
    reply_id = _insert_reply(db, "old")
    _set_detected_at(db, reply_id, "2026-04-22 10:00:00")

    report = build_reply_stale_context_report(db, max_age_hours=24, now=NOW)

    finding = report["findings"][0]
    assert finding["id"] == reply_id
    assert finding["age_hours"] == 26.0
    assert finding["stale_status"] == "stale"
    assert finding["recommended_action"] == "refresh_context"
    assert finding["reasons"] == ["inbound reply is 26h old"]


def test_fresh_draft_without_risks_is_ready_for_review(db):
    reply_id = _insert_reply(db, "fresh")
    _set_detected_at(db, reply_id, "2026-04-23 10:00:00")

    report = build_reply_stale_context_report(db, max_age_hours=24, now=NOW)

    assert report["counts"]["fresh"] == 1
    finding = report["findings"][0]
    assert finding["id"] == reply_id
    assert finding["stale"] is False
    assert finding["recommended_action"] == "ready_for_review"
    assert finding["reasons"] == []


def test_missing_source_context_requests_refresh(db):
    reply_id = _insert_reply(db, "missing-source", our_post_text="  ")
    _set_detected_at(db, reply_id, "2026-04-23 11:00:00")

    report = build_reply_stale_context_report(db, max_age_hours=24, now=NOW)

    finding = report["findings"][0]
    assert finding["stale_status"] == "stale"
    assert finding["recommended_action"] == "refresh_context"
    assert "original post text is missing" in finding["reasons"]
    assert finding["our_post_text_present"] is False


def test_temporal_language_in_old_draft_requests_redraft(db):
    row = {
        "id": 4,
        "status": "pending",
        "platform": "x",
        "inbound_tweet_id": "temporal",
        "inbound_author_handle": "alice",
        "inbound_text": "Can you clarify?",
        "our_post_text": "Original post",
        "draft_text": "I can take a look this morning. Just now I saw the logs.",
        "detected_at": "2026-04-22 06:00:00",
    }

    finding = inspect_reply_stale_context(row, max_age_hours=24, now=NOW)

    assert finding["recommended_action"] == "redraft"
    assert finding["temporal_phrases"] == ["this morning", "just now"]
    assert "draft uses outdated temporal language: this morning, just now" in finding[
        "reasons"
    ]


def test_deleted_or_unavailable_parent_metadata_forces_manual_hold(db):
    reply_id = _insert_reply(
        db,
        "deleted",
        platform_metadata=json.dumps(
            {
                "root_post_text": "Original post",
                "parent": {"deleted": True},
                "parent_post": {"status": "unavailable"},
            }
        ),
    )
    _set_detected_at(db, reply_id, "2026-04-23 11:00:00")

    report = build_reply_stale_context_report(db, max_age_hours=24, now=NOW)

    finding = report["findings"][0]
    assert finding["recommended_action"] == "hold_for_manual_review"
    assert finding["stale_status"] == "stale"
    assert "parent.deleted" in finding["metadata_flags"]
    assert "parent_post.status" in finding["metadata_flags"]


def test_status_filter_limits_rows_without_mutating(db):
    pending_id = _insert_reply(db, "pending", status="pending")
    approved_id = _insert_reply(db, "approved", status="approved")
    _set_detected_at(db, pending_id, "2026-04-22 10:00:00")
    _set_detected_at(db, approved_id, "2026-04-22 10:00:00")

    report = build_reply_stale_context_report(
        db,
        max_age_hours=24,
        status_filter=["approved"],
        now=NOW,
    )
    statuses = db.conn.execute(
        "SELECT inbound_tweet_id, status FROM reply_queue ORDER BY id"
    ).fetchall()

    assert [item["reply_id"] for item in report["findings"]] == ["approved"]
    assert [(row["inbound_tweet_id"], row["status"]) for row in statuses] == [
        ("pending", "pending"),
        ("approved", "approved"),
    ]


def test_partial_reply_queue_schema_does_not_crash_builder():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            draft_text TEXT,
            detected_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_queue (id, status, draft_text, detected_at) VALUES (?, ?, ?, ?)",
        (1, "pending", "Thanks today", "2026-04-22 06:00:00"),
    )

    report = build_reply_stale_context_report(conn, max_age_hours=24, now=NOW)

    assert report["total"] == 1
    assert report["findings"][0]["recommended_action"] == "refresh_context"


def test_text_format_lists_actions_and_reasons(db):
    reply_id = _insert_reply(db, "text", draft_text="I checked this today.")
    _set_detected_at(db, reply_id, "2026-04-22 06:00:00")

    text = format_reply_stale_context_text(
        build_reply_stale_context_report(db, max_age_hours=24, now=NOW)
    )

    assert "Reply stale-context audit" in text
    assert f"#{reply_id} stale 30.0h redraft" in text
    assert "draft uses outdated temporal language: today" in text


def test_cli_json_output(capsys):
    class FakeDb:
        conn = sqlite3.connect(":memory:")

    FakeDb.conn.row_factory = sqlite3.Row
    FakeDb.conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            platform TEXT,
            inbound_tweet_id TEXT,
            inbound_author_handle TEXT,
            inbound_text TEXT,
            our_post_text TEXT,
            draft_text TEXT,
            detected_at TEXT
        )"""
    )
    FakeDb.conn.execute(
        """INSERT INTO reply_queue
           (id, status, platform, inbound_tweet_id, inbound_author_handle,
            inbound_text, our_post_text, draft_text, detected_at)
           VALUES (1, 'pending', 'x', 'reply-1', 'alice',
                   'Question', 'Original', 'I checked this yesterday.',
                   '2026-04-22 06:00:00')"""
    )

    fixed_report = build_reply_stale_context_report(
        FakeDb(),
        max_age_hours=24,
        now=NOW,
    )

    with patch("reply_stale_context.script_context", _mock_script_context(FakeDb())), patch(
        "reply_stale_context.build_reply_stale_context_report",
        return_value=fixed_report,
    ):
        result = main(["--max-age-hours", "24", "--format", "json"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["max_age_hours"] == 24
    assert payload["findings"][0]["recommended_action"] == "redraft"
