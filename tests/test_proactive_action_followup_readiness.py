from datetime import datetime, timezone
import sqlite3

from evaluation.proactive_action_followup_readiness import build_proactive_action_followup_readiness_report, build_proactive_action_followup_readiness_report_from_db

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_proactive_action_followup_readiness():
    report = build_proactive_action_followup_readiness_report([
        {"row_type": "action", "action_id": 1, "status": "posted", "target_handle": "@a", "posted_at": "2026-05-01T00:00:00+00:00"},
        {"row_type": "action", "action_id": 2, "status": "approved", "target_handle": "@a", "approved_at": "2026-05-01T01:00:00+00:00"},
        {"row_type": "reminder", "reminder_id": 10, "action_id": 9},
        {"row_type": "reminder", "reminder_id": 11, "action_id": 2, "due_at": "2026-04-30T00:00:00+00:00"},
    ], now=NOW)
    assert {"missing_followup_reminder", "orphan_followup_reminder", "premature_followup_due", "duplicate_target_window"} <= {f["reason"] for f in report["findings"]}
    assert build_proactive_action_followup_readiness_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["proactive_actions", "reply_followup_reminders"]
