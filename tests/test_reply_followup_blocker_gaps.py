from __future__ import annotations
from datetime import datetime, timezone
import sqlite3
from engagement.reply_followup_blocker_gaps import build_reply_followup_blocker_gaps_report, build_reply_followup_blocker_gaps_report_from_db

NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)

def test_flags_missing_short_placeholder_and_generic_repeated_reasons():
    rows = [
        {"id": 1, "status": "blocked", "blocker_reason": "", "created_at": "2026-04-30T00:00:00+00:00"},
        {"id": 2, "status": "deferred", "blocker_reason": "tbd", "created_at": NOW.isoformat()},
        {"id": 3, "status": "blocked", "blocker_reason": "short", "created_at": NOW.isoformat()},
        {"id": 4, "status": "blocked", "blocker_reason": "waiting input", "created_at": NOW.isoformat()},
        {"id": 5, "status": "blocked", "blocker_reason": "waiting input", "created_at": NOW.isoformat()},
    ]
    r = build_reply_followup_blocker_gaps_report(rows, min_reason_length=4, now=NOW)
    assert r["artifact_type"] == "reply_followup_blocker_gaps"
    assert {f["gap_reason"] for f in r["findings"]} >= {"missing_blocker_reason", "placeholder_blocker_reason", "repeated_generic_blocker_reason"}

def test_db_uses_proactive_actions_fallback():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE proactive_actions (id INTEGER, status TEXT, blocker_reason TEXT, created_at TEXT)")
    c.execute("INSERT INTO proactive_actions VALUES (1,'blocked',NULL,'2026-04-30T00:00:00+00:00')")
    r = build_reply_followup_blocker_gaps_report_from_db(c, now=NOW)
    assert r["findings"][0]["followup_id"] == 1
