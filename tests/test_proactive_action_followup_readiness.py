from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.proactive_action_followup_readiness import build_proactive_action_followup_readiness_report, build_proactive_action_followup_readiness_report_from_db

NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_followup_readiness.py"
spec = importlib.util.spec_from_file_location("proactive_action_followup_readiness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_followup_gaps():
    report = build_proactive_action_followup_readiness_report([
        {"action_id": 1, "action_status": "posted", "target_author_handle": "a", "created_at": "2026-05-20T00:00:00+00:00"},
        {"reminder_id": 2, "reminder_target_handle": "b", "due_at": "2026-05-19T00:00:00+00:00"},
        {"action_id": 3, "reminder_id": 3, "action_status": "approved", "target_author_handle": "a", "due_at": "2026-05-19T00:00:00+00:00", "reviewed_at": "2026-05-20T00:00:00+00:00", "created_at": "2026-05-20T01:00:00+00:00"},
    ], window_hours=48, now=NOW)
    assert report["artifact_type"] == "proactive_action_followup_readiness"
    assert report["totals"]["by_reason"]["missing_followup_reminder"] == 1
    assert report["totals"]["by_reason"]["orphan_followup_reminder"] == 1
    assert report["totals"]["by_reason"]["premature_followup_due"] >= 1
    assert report["totals"]["by_reason"]["duplicate_target_window"] == 1


def test_db_loader_and_cli(tmp_path, capsys):
    path = tmp_path / "pa.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript("""CREATE TABLE proactive_actions (id INTEGER, status TEXT, target_author_handle TEXT, created_at TEXT, reviewed_at TEXT, posted_at TEXT);
    CREATE TABLE reply_followup_reminders (id INTEGER, target_handle TEXT, source_type TEXT, source_id INTEGER, source_action_id INTEGER, due_at TEXT);
    INSERT INTO proactive_actions VALUES (1, 'posted', 'a', '2026-05-20T00:00:00+00:00', NULL, '2026-05-20T01:00:00+00:00');""")
    conn.commit()
    assert build_proactive_action_followup_readiness_report_from_db(conn, now=NOW)["totals"]["by_reason"]["missing_followup_reminder"] == 1
    assert script.main(["--db", str(path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "proactive_action_followup_readiness"
