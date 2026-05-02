"""Tests for reply review latency analytics."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.reply_review_latency import (  # noqa: E402
    build_reply_review_latency_report,
    format_reply_review_latency_json,
    format_reply_review_latency_text,
)
from reply_review_latency import main  # noqa: E402


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
        draft_text="Thanks",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_report_covers_reviewed_posted_rejected_and_pending_replies(db):
    reviewed = _insert_reply(
        db,
        "reviewed",
        platform="x",
        priority="normal",
        intent="appreciation",
        status="pending",
    )
    posted = _insert_reply(
        db,
        "posted",
        platform="x",
        priority="high",
        intent="question",
        status="posted",
    )
    rejected = _insert_reply(
        db,
        "rejected",
        platform="bluesky",
        priority="low",
        intent="objection",
        status="dismissed",
    )
    pending = _insert_reply(
        db,
        "pending",
        platform="bluesky",
        priority="normal",
        intent="question",
        status="pending",
    )

    _set_detected_at(db, reviewed, "2026-04-23 06:00:00")
    _set_detected_at(db, posted, "2026-04-23 00:00:00")
    _set_detected_at(db, rejected, "2026-04-22 12:00:00")
    _set_detected_at(db, pending, "2026-04-21 12:00:00")
    db.record_reply_review_event(
        reviewed,
        "edited",
        old_status="pending",
        new_status="pending",
        created_at="2026-04-23T10:00:00+00:00",
    )
    db.record_reply_review_event(
        posted,
        "approved",
        old_status="pending",
        new_status="approved",
        created_at="2026-04-23T02:00:00+00:00",
    )
    db.record_reply_review_event(
        posted,
        "posted",
        old_status="approved",
        new_status="posted",
        created_at="2026-04-23T05:00:00+00:00",
    )
    db.record_reply_review_event(
        rejected,
        "rejected",
        old_status="pending",
        new_status="dismissed",
        created_at="2026-04-23T00:00:00+00:00",
    )

    report = build_reply_review_latency_report(
        db,
        days=7,
        sla_hours=24,
        group_by="platform",
        now=NOW,
    )

    assert report["overall"]["counts"] == {
        "total": 4,
        "reviewed": 3,
        "approved": 0,
        "posted": 1,
        "rejected": 1,
        "pending": 1,
        "breached": 1,
    }
    assert report["overall"]["latency_hours"]["first_review"] == {
        "count": 4,
        "median": 8.0,
        "p90": 37.2,
    }
    assert report["overall"]["latency_hours"]["approval"]["median"] == 2.0
    assert report["overall"]["latency_hours"]["rejection"]["median"] == 12.0
    assert report["overall"]["latency_hours"]["posting"]["median"] == 5.0
    assert report["overall"]["breached_item_ids"] == [pending]

    by_platform = {group["group"]: group for group in report["groups"]}
    assert by_platform["x"]["counts"]["posted"] == 1
    assert by_platform["bluesky"]["counts"]["pending"] == 1
    assert by_platform["bluesky"]["breached_item_ids"] == [pending]

    items = {item["id"]: item for item in report["items"]}
    assert items[reviewed]["status"] == "reviewed"
    assert items[posted]["status"] == "posted"
    assert items[rejected]["status"] == "rejected"
    assert items[pending]["status"] == "pending"
    assert items[pending]["latency_hours"]["first_review"] == 48.0
    assert items[pending]["pending"] is True


def test_missing_review_events_are_pending_even_when_row_status_is_posted(db):
    missing = _insert_reply(db, "missing-events", status="posted", platform="x")
    _set_detected_at(db, missing, "2026-04-22 12:00:00")

    report = build_reply_review_latency_report(
        db,
        days=7,
        sla_hours=12,
        group_by="priority",
        now=NOW,
    )

    assert report["items"][0]["id"] == missing
    assert report["items"][0]["status"] == "pending"
    assert report["items"][0]["pending"] is True
    assert report["items"][0]["latency_hours"]["first_review"] == 24.0
    assert report["items"][0]["breached"] is True
    assert report["groups"][0]["group"] == "normal"


def test_text_and_json_output_are_deterministic(db):
    reply = _insert_reply(
        db,
        "tw-1",
        platform="x",
        priority="high",
        intent="question",
        status="posted",
    )
    _set_detected_at(db, reply, "2026-04-23 00:00:00")
    db.record_reply_review_event(
        reply,
        "approved",
        old_status="pending",
        new_status="approved",
        created_at="2026-04-23T02:00:00+00:00",
    )
    db.record_reply_review_event(
        reply,
        "posted",
        old_status="approved",
        new_status="posted",
        created_at="2026-04-23T05:00:00+00:00",
    )

    report = build_reply_review_latency_report(
        db,
        days=7,
        sla_hours=6,
        group_by="intent",
        now=NOW,
    )

    assert json.loads(format_reply_review_latency_json(report))["filters"] == {
        "days": 7,
        "group_by": "intent",
        "lookback_end": "2026-04-23T12:00:00+00:00",
        "lookback_start": "2026-04-16T12:00:00+00:00",
        "sla_hours": 6.0,
    }
    assert format_reply_review_latency_text(report) == "\n".join(
        [
            "Reply Review Latency Report",
            "Generated: 2026-04-23T12:00:00+00:00",
            (
                "Lookback: 7 days "
                "(2026-04-16T12:00:00+00:00 to 2026-04-23T12:00:00+00:00)"
            ),
            "SLA: 6h first review",
            "Group by: intent",
            "Rows: 1 reviewed=1 posted=1 rejected=0 pending=0 breached=0",
            (
                "Overall latency h: review median=2.00 p90=2.00; "
                "approval median=2.00 p90=2.00; rejection median=n/a p90=n/a; "
                "posting median=5.00 p90=5.00"
            ),
            "Breached ids: none",
            "",
            "Groups:",
            (
                "  Group            Total   Rev  Post Reject  Pend Breach   Med h   P90 h  "
                "Breached ids"
            ),
            "  --------------------------------------------------------------------------------------",
            "  question             1     1     1      0     0      0    2.00    2.00  none",
        ]
    )


def test_partial_schema_and_empty_schema_are_handled():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            detected_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_queue (id, detected_at) VALUES (?, ?)",
        (7, "2026-04-23 00:00:00"),
    )

    report = build_reply_review_latency_report(conn, days=7, sla_hours=24, now=NOW)

    assert report["overall"]["counts"]["pending"] == 1
    assert report["items"][0]["platform"] == "x"
    assert report["items"][0]["intent"] == "other"

    empty = build_reply_review_latency_report(
        sqlite3.connect(":memory:"),
        days=7,
        sla_hours=24,
        now=NOW,
    )
    assert empty["items"] == []
    assert empty["overall"]["counts"]["total"] == 0


def test_cli_json_output(capsys):
    class FakeDb:
        def __init__(self):
            self.conn = sqlite3.connect(":memory:")
            self.conn.execute(
                """CREATE TABLE reply_queue (
                    id INTEGER PRIMARY KEY,
                    detected_at TEXT,
                    platform TEXT,
                    priority TEXT,
                    intent TEXT
                )"""
            )
            self.conn.execute(
                """INSERT INTO reply_queue
                   (id, detected_at, platform, priority, intent)
                   VALUES (1, '2026-05-02 00:00:00', 'x', 'high', 'question')"""
            )
            self.conn.execute(
                """CREATE TABLE reply_review_events (
                    id INTEGER PRIMARY KEY,
                    reply_queue_id INTEGER,
                    event_type TEXT,
                    new_status TEXT,
                    created_at TEXT
                )"""
            )

    with patch("reply_review_latency.script_context", _mock_script_context(FakeDb())):
        assert main(["--format", "json", "--days", "7", "--sla-hours", "6"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days"] == 7
    assert payload["filters"]["sla_hours"] == 6.0
    assert payload["groups"][0]["group"] == "x"


@pytest.mark.parametrize(
    "argv",
    [
        ["--days", "0"],
        ["--sla-hours", "0"],
    ],
)
def test_invalid_cli_arguments_exit_2(argv):
    with pytest.raises(SystemExit) as excinfo:
        main(argv)

    assert excinfo.value.code == 2
