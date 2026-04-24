"""Tests for reply backlog triage reports."""

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

from engagement.reply_backlog import build_reply_backlog_report, format_text_report
from reply_backlog import main


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


def test_report_groups_age_quality_duplicate_and_ready_buckets(db):
    overdue = _insert_reply(
        db,
        "overdue",
        priority="high",
        intent="bug_report",
        inbound_author_handle="zoe",
        draft_text="I can look into that failure path.",
    )
    stale = _insert_reply(
        db,
        "stale",
        priority="normal",
        intent="question",
        inbound_author_handle="yuki",
        draft_text="The short version is that retries need a budget.",
    )
    regen = _insert_reply(
        db,
        "regen",
        priority="normal",
        inbound_author_handle="mona",
        draft_text="This probably needs a more specific response.",
        quality_score=5.5,
        quality_flags=json.dumps(["generic"]),
    )
    original = _insert_reply(db, "dup-original", inbound_author_handle="bob")
    duplicate = _insert_reply(
        db,
        "dup-candidate",
        inbound_author_handle="@bob",
        draft_text="thanks for sharing this!",
    )
    ready = _insert_reply(
        db,
        "ready",
        priority="normal",
        inbound_author_handle="nina",
        draft_text="Useful detail.",
    )
    low = _insert_reply(
        db,
        "low",
        priority="low",
        inbound_author_handle="li",
        draft_text="Low priority acknowledgement.",
    )
    posted = _insert_reply(
        db,
        "posted",
        status="posted",
        inbound_author_handle="posted",
        draft_text="Already handled elsewhere.",
    )

    _set_detected_at(db, overdue, "2026-04-22 06:00:00")
    _set_detected_at(db, stale, "2026-04-21 10:00:00")
    _set_detected_at(db, regen, "2026-04-23 08:00:00")
    _set_detected_at(db, original, "2026-04-23 07:00:00")
    _set_detected_at(db, duplicate, "2026-04-23 09:00:00")
    _set_detected_at(db, ready, "2026-04-23 10:00:00")
    _set_detected_at(db, low, "2026-04-22 06:00:00")
    _set_detected_at(db, posted, "2026-04-22 06:00:00")

    report = build_reply_backlog_report(db, now=NOW)

    assert report["counts"] == {
        "overdue": 1,
        "needs_regeneration": 1,
        "duplicate_risk": 2,
        "stale": 1,
        "ready": 1,
    }
    assert report["total_pending"] == 6
    assert report["by_priority"] == {"high": 1, "normal": 5}
    assert report["by_classification"]["bug_report"] == 1
    assert report["buckets"]["overdue"][0]["reply_id"] == "overdue"
    assert report["buckets"]["needs_regeneration"][0]["reply_id"] == "regen"
    assert {item["reply_id"] for item in report["buckets"]["duplicate_risk"]} == {
        "dup-original",
        "dup-candidate",
    }
    assert report["buckets"]["duplicate_risk"][0]["duplicate_match"]["reason"] == "same_author"
    assert report["buckets"]["stale"][0]["reply_id"] == "stale"
    assert report["buckets"]["ready"][0]["reply_id"] == "ready"


def test_text_report_is_concise_and_sorted_by_urgency(db):
    stale = _insert_reply(
        db,
        "stale",
        priority="normal",
        inbound_author_handle="sara",
        draft_text="This one can wait.",
    )
    overdue = _insert_reply(
        db,
        "overdue",
        priority="high",
        inbound_author_handle="taro",
        draft_text="This needs a fast answer.",
    )
    _set_detected_at(db, stale, "2026-04-21 10:00:00")
    _set_detected_at(db, overdue, "2026-04-22 06:00:00")

    text = format_text_report(build_reply_backlog_report(db, now=NOW))

    assert text.index("Overdue") < text.index("Stale")
    assert "#2 30.0h high" in text
    assert "#1 50.0h normal" in text
    assert len(text.splitlines()) < 12


def test_include_low_priority_option_adds_low_priority_replies(db):
    low = _insert_reply(db, "low", priority="low")
    _set_detected_at(db, low, "2026-04-21 06:00:00")

    without_low = build_reply_backlog_report(db, now=NOW)
    with_low = build_reply_backlog_report(db, now=NOW, include_low_priority=True)

    assert without_low["total_pending"] == 0
    assert with_low["total_pending"] == 1
    assert with_low["buckets"]["stale"][0]["reply_id"] == "low"


def test_partial_reply_queue_schema_does_not_crash_builder():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            draft_text TEXT,
            detected_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_queue (id, draft_text, detected_at) VALUES (?, ?, ?)",
        (1, "Thanks for this", "2026-04-23 11:00:00"),
    )

    report = build_reply_backlog_report(conn, now=NOW)

    assert report["total_pending"] == 1
    item = report["buckets"]["ready"][0]
    assert item["priority"] == "normal"
    assert item["intent"] == "other"
    assert item["platform"] == "x"


def test_days_min_age_and_limit_filters_are_stable(db):
    fresh = _insert_reply(db, "fresh", priority="normal")
    old = _insert_reply(db, "old", priority="high")
    _set_detected_at(db, fresh, "2026-04-23 11:30:00")
    _set_detected_at(db, old, "2026-04-22 06:00:00")

    report = build_reply_backlog_report(db, now=NOW, min_age_hours=1, limit=1)
    encoded = json.dumps(report, sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["total_pending"] == 1
    assert decoded["buckets"]["overdue"][0]["reply_id"] == "old"
    assert decoded["filters"] == {
        "days": 7,
        "include_low_priority": False,
        "limit": 1,
        "min_age_hours": 1,
    }


def test_cli_json_output(capsys):
    class FakeDb:
        conn = sqlite3.connect(":memory:")

    FakeDb.conn.row_factory = sqlite3.Row
    FakeDb.conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            priority TEXT,
            detected_at TEXT,
            draft_text TEXT
        )"""
    )
    FakeDb.conn.execute(
        """INSERT INTO reply_queue
           (id, status, priority, detected_at, draft_text)
           VALUES (1, 'pending', 'normal', '2026-04-23 10:00:00', 'Thanks')"""
    )

    with patch("reply_backlog.script_context", _mock_script_context(FakeDb())):
        assert main(["--json", "--days", "3", "--min-age-hours", "1"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days"] == 3
    assert payload["total_pending"] >= 0
