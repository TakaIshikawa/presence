from __future__ import annotations
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_personalization_token_leakage import build_newsletter_personalization_token_leakage_report, build_newsletter_personalization_token_leakage_report_from_db, format_newsletter_personalization_token_leakage_json, format_newsletter_personalization_token_leakage_text

NOW = datetime(2026, 5, 20, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_personalization_token_leakage.py"
spec = importlib.util.spec_from_file_location("script_newsletter_personalization_token_leakage", SCRIPT)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)


def test_multiple_token_syntaxes_and_fields():
    report = build_newsletter_personalization_token_leakage_report([
        {"id": "n1", "subject": "Hi {{first_name}}", "preheader": "For %EMAIL%", "body": "About [[company]] and {{broken"},
    ], now=NOW)
    assert report["summary"]["leakage_count"] == 4
    assert {i["field"] for i in report["issues"]} == {"subject", "preheader", "body"}
    assert {i["issue_type"] for i in report["issues"]} == {"unresolved_token", "malformed_token"}
    assert json.loads(format_newsletter_personalization_token_leakage_json(report))["artifact_type"] == "newsletter_personalization_token_leakage"
    assert "Newsletter Personalization Token Leakage" in format_newsletter_personalization_token_leakage_text(report)


def test_code_blocks_and_escaped_literals_are_allowed():
    report = build_newsletter_personalization_token_leakage_report([
        {"id": "n1", "subject": r"\{{first_name}}", "preheader": "", "body": "```{{first_name}}```\nUse `%EMAIL%` literally."},
    ], now=NOW)
    assert report["issues"] == []


def test_db_builder_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:"); conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE newsletter_drafts (id TEXT, title TEXT, subject TEXT, preheader TEXT, body TEXT)")
    conn.execute("INSERT INTO newsletter_drafts VALUES ('n1', 'T', 'Hi {{first_name}}', '', 'ok')")
    report = build_newsletter_personalization_token_leakage_report_from_db(conn, now=NOW)
    assert report["issues"][0]["newsletter_id"] == "n1"
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); conn.commit(); conn.backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0
    assert "n1 | subject | {{first_name}}" in capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2
