from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3

from engagement.reply_review_audit_gaps import (
    build_reply_review_audit_gaps_report,
    format_reply_review_audit_gaps_json,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)


def _reply(db, status="pending", detected_hours_ago=1):
    db.conn.execute(
        """INSERT INTO reply_queue
           (inbound_tweet_id, inbound_text, our_tweet_id, status, detected_at, reviewed_at, posted_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            f"in-{status}-{detected_hours_ago}-{datetime.now().timestamp()}",
            "hello",
            "our",
            status,
            (NOW - timedelta(hours=detected_hours_ago)).isoformat(),
            NOW.isoformat() if status in {"approved", "posted"} else None,
            NOW.isoformat() if status == "posted" else None,
        ),
    )
    db.conn.commit()
    return db.conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _event(db, reply_id, event_type, new_status):
    db.conn.execute(
        """INSERT INTO reply_review_events (reply_queue_id, event_type, new_status, created_at)
           VALUES (?, ?, ?, ?)""",
        (reply_id, event_type, new_status, NOW.isoformat()),
    )
    db.conn.commit()


def test_report_flags_missing_events_stale_pending_and_status_mismatch(db):
    approved = _reply(db, "approved")
    posted = _reply(db, "posted")
    pending = _reply(db, "pending", detected_hours_ago=72)
    mismatch = _reply(db, "pending")
    _event(db, mismatch, "approved", "approved")

    payload = json.loads(format_reply_review_audit_gaps_json(
        build_reply_review_audit_gaps_report(db, stale_pending_hours=48, now=NOW)
    ))

    assert payload["artifact_type"] == "reply_review_audit_gaps"
    assert payload["status_counts"]["pending"] == 2
    assert payload["event_counts"]["approved"] == 1
    by_type = {finding["finding_type"]: finding for finding in payload["findings"]}
    assert by_type["approved_without_event"]["reply_id"] == approved
    assert by_type["posted_without_event"]["reply_id"] == posted
    assert by_type["stale_pending_without_event"]["reply_id"] == pending
    assert by_type["status_event_mismatch"]["reply_id"] == mismatch
    assert set(payload["representative_reply_ids"]) == {approved, posted, pending, mismatch}


def test_missing_reply_review_events_table_is_reported_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY, status TEXT, detected_at TEXT, reviewed_at TEXT, posted_at TEXT
        )"""
    )
    conn.execute("INSERT INTO reply_queue VALUES (1, 'approved', '2026-05-13T10:00:00+00:00', NULL, NULL)")

    report = build_reply_review_audit_gaps_report(conn, now=NOW)

    assert report["missing_tables"] == ["reply_review_events"]
    assert report["findings"][0]["finding_type"] == "approved_without_event"
