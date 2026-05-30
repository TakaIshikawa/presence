from __future__ import annotations
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.generated_content_cta_clarity import build_generated_content_cta_clarity_report, build_generated_content_cta_clarity_report_from_db, format_generated_content_cta_clarity_json, format_generated_content_cta_clarity_text
NOW = datetime(2026, 5, 20, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "generated_content_cta_clarity.py"
spec = importlib.util.spec_from_file_location("script_generated_content_cta_clarity", SCRIPT)
script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def test_clear_missing_vague_and_conflicting_ctas():
    report = build_generated_content_cta_clarity_report([
        {"id": "clear", "content": "Register for the workshop at https://example.com/workshop."},
        {"id": "missing", "content": "A useful update with no ask."},
        {"id": "vague", "content": "Check it out: https://example.com"},
        {"id": "conflict", "content": "Download the guide and subscribe to updates."},
    ], now=NOW)
    by_id = {i["content_id"]: i for i in report["items"]}
    assert "clear" not in by_id
    assert by_id["missing"]["issue_reasons"] == ["missing_cta"]
    assert {"missing_cta", "vague_cta", "missing_destination_context"} <= set(by_id["vague"]["issue_reasons"])
    assert "multiple_competing_ctas" in by_id["conflict"]["issue_reasons"]
    assert json.loads(format_generated_content_cta_clarity_json(report))["artifact_type"] == "generated_content_cta_clarity"
    assert "Generated Content CTA Clarity" in format_generated_content_cta_clarity_text(report)

def test_db_builder_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:"); conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id TEXT, content_type TEXT, content TEXT)")
    conn.execute("INSERT INTO generated_content VALUES ('g1', 'post', 'No CTA here')")
    report = build_generated_content_cta_clarity_report_from_db(conn, now=NOW)
    assert report["items"][0]["content_id"] == "g1"
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); conn.commit(); conn.backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0
    assert "missing_cta" in capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2
