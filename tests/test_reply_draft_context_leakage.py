from __future__ import annotations
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.reply_draft_context_leakage import build_reply_draft_context_leakage_report, build_reply_draft_context_leakage_report_from_db, format_reply_draft_context_leakage_json, format_reply_draft_context_leakage_text
NOW = datetime(2026, 5, 20, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_context_leakage.py"
spec = importlib.util.spec_from_file_location("script_reply_draft_context_leakage", SCRIPT)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_safe_replies_and_leak_patterns():
    report = build_reply_draft_context_leakage_report([
        {"id": "safe", "body": "Great to hear from you again. The public launch note was helpful."},
        {"id": "bad", "body": "Internal note: upsell later. CRM:vip email jane@internal.test [operator: soften tone]"},
    ], now=NOW)
    assert report["summary"]["leak_count"] == 4
    assert {i["reason"] for i in report["issues"]} == {"internal_note_marker", "crm_tag", "private_identifier", "operator_instruction"}
    assert all(i["reply_id"] != "safe" for i in report["issues"])
    assert json.loads(format_reply_draft_context_leakage_json(report))["artifact_type"] == "reply_draft_context_leakage"
    assert "Reply Draft Context Leakage" in format_reply_draft_context_leakage_text(report)

def test_db_builder_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:"); conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_drafts (id TEXT, target_id TEXT, body TEXT)")
    conn.execute("INSERT INTO reply_drafts VALUES ('r1', 'p1', 'TAG:hot lead')")
    report = build_reply_draft_context_leakage_report_from_db(conn, now=NOW)
    assert report["issues"][0]["reply_id"] == "r1"
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); conn.commit(); conn.backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0
    assert "crm_tag" in capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2
