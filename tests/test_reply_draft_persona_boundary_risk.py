from __future__ import annotations
from datetime import datetime, timezone
import sqlite3
from engagement.reply_draft_persona_boundary_risk import build_reply_draft_persona_boundary_risk_report, build_reply_draft_persona_boundary_risk_report_from_db

NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)

def test_flags_risky_first_person_but_not_benign():
    rows = [
        {"id": 1, "status": "draft", "body": "I think this is useful and I hope it helps.", "created_at": NOW.isoformat()},
        {"id": 2, "status": "draft", "body": "I work at Example and I can guarantee the outcome.", "created_at": NOW.isoformat()},
    ]
    r = build_reply_draft_persona_boundary_risk_report(rows, now=NOW)
    assert r["artifact_type"] == "reply_draft_persona_boundary_risk"
    assert {f["risk_type"] for f in r["findings"]} == {"employment_claim", "guaranteed_outcome"}
    assert all("matched_phrase" in f and "severity" in f for f in r["findings"])

def test_db_reports_missing_text_columns():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE reply_drafts (id INTEGER, status TEXT)")
    r = build_reply_draft_persona_boundary_risk_report_from_db(c, now=NOW)
    assert r["missing_columns"] == {"reply_drafts": ["body|content|text|draft_text|reply_text"]}
