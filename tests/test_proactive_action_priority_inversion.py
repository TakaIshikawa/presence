from __future__ import annotations
from datetime import datetime, timezone
import sqlite3
from engagement.proactive_action_priority_inversion import build_proactive_action_priority_inversion_report, build_proactive_action_priority_inversion_report_from_db, priority_rank

NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)

def test_pairs_high_priority_blocked_by_lower_priority_overtaker():
    rows = [
        {"id": 1, "status": "open", "priority": "P1", "created_at": "2026-04-20T00:00:00+00:00", "due_at": "2026-05-03T00:00:00+00:00"},
        {"id": 2, "status": "open", "priority": "low", "created_at": "2026-04-25T00:00:00+00:00", "reviewed_at": "2026-04-30T00:00:00+00:00"},
    ]
    r = build_proactive_action_priority_inversion_report(rows, now=NOW)
    assert r["artifact_type"] == "proactive_action_priority_inversion"
    assert r["findings"][0]["blocked_action_id"] == 1
    assert r["findings"][0]["overtaking_action_id"] == 2
    assert priority_rank({"priority": "critical"}) == 1
    assert priority_rank({"priority_score": 4}) == 4

def test_db_falls_back_to_created_at_when_schedule_columns_missing():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE proactive_actions (id INTEGER, status TEXT, severity TEXT, created_at TEXT)")
    c.execute("INSERT INTO proactive_actions VALUES (1,'open','high','2026-04-20T00:00:00+00:00')")
    c.execute("INSERT INTO proactive_actions VALUES (2,'open','low','2026-04-25T00:00:00+00:00')")
    r = build_proactive_action_priority_inversion_report_from_db(c, now=NOW)
    assert r["missing_columns"] == {}
    assert r["summary"]["actions_scanned"] == 2
