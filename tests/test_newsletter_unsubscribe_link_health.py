from __future__ import annotations
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_unsubscribe_link_health import build_newsletter_unsubscribe_link_health_report, build_newsletter_unsubscribe_link_health_report_from_db, format_newsletter_unsubscribe_link_health_json, format_newsletter_unsubscribe_link_health_text

NOW = datetime(2026, 5, 20, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_unsubscribe_link_health.py"
spec = importlib.util.spec_from_file_location("script_newsletter_unsubscribe_link_health", SCRIPT)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_html_markdown_placeholder_conflict_and_healthy():
    report = build_newsletter_unsubscribe_link_health_report([
        {"id": "missing", "body": "hello"},
        {"id": "bad", "body": "[unsubscribe](http://example.com/unsubscribe) <a href='https://mail.test/unsub'>Unsubscribe</a>"},
        {"id": "ok", "body": "<a href='https://mail.test/unsub'>Unsubscribe</a>"},
    ], now=NOW)
    by_id = {i["newsletter_id"]: i for i in report["issues"]}
    assert by_id["missing"]["issue_type"] == "missing_unsubscribe_link"
    assert {"non_https_unsubscribe_url", "placeholder_unsubscribe_url", "conflicting_unsubscribe_destinations"} <= {i["issue_type"] for i in report["issues"]}
    assert "ok" not in by_id
    assert json.loads(format_newsletter_unsubscribe_link_health_json(report))["artifact_type"] == "newsletter_unsubscribe_link_health"
    assert "Newsletter Unsubscribe Link Health" in format_newsletter_unsubscribe_link_health_text(report)

def test_db_builder_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:"); conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE newsletter_drafts (id TEXT, subject TEXT, body TEXT)")
    conn.execute("INSERT INTO newsletter_drafts VALUES ('n1', 'S', 'no footer')")
    report = build_newsletter_unsubscribe_link_health_report_from_db(conn, now=NOW)
    assert report["issues"][0]["newsletter_id"] == "n1"
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); conn.commit(); conn.backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0
    assert "missing_unsubscribe_link" in capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2
