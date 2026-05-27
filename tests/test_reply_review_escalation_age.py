from __future__ import annotations
from datetime import datetime, timezone
import sqlite3
from engagement.reply_review_escalation_age import build_reply_review_escalation_age_report, build_reply_review_escalation_age_report_from_db

NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)

def test_escalation_rows_older_than_sla_sorted_by_age():
    rows = [
        {"id": 1, "status": "escalated", "platform": "x", "created_at": "2026-04-29T00:00:00+00:00"},
        {"id": 2, "status": "human_review", "platform": "x", "created_at": "2026-04-30T00:00:00+00:00"},
        {"id": 3, "status": "sent", "created_at": "2026-04-20T00:00:00+00:00"},
    ]
    r = build_reply_review_escalation_age_report(rows, sla_hours=24, now=NOW)
    assert r["artifact_type"] == "reply_review_escalation_age"
    assert [f["reply_draft_id"] for f in r["findings"]] == [1, 2]
    assert r["findings"][0]["age_hours"] > r["findings"][1]["age_hours"]

def test_db_reads_optional_review_events():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE reply_drafts (id INTEGER, status TEXT, platform TEXT, created_at TEXT)")
    c.execute("CREATE TABLE reply_review_events (reply_draft_id INTEGER, reason TEXT, created_at TEXT)")
    c.execute("INSERT INTO reply_drafts VALUES (1,'escalated','x','2026-04-20T00:00:00+00:00')")
    c.execute("INSERT INTO reply_review_events VALUES (1,'legal','2026-04-29T00:00:00+00:00')")
    r = build_reply_review_escalation_age_report_from_db(c, sla_hours=24, now=NOW)
    assert r["findings"][0]["reason"] == "legal"
