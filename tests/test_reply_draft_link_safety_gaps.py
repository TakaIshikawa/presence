from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.reply_draft_link_safety_gaps import build_reply_draft_link_safety_gaps_report_from_db, format_reply_draft_link_safety_gaps_json, format_reply_draft_link_safety_gaps_text
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_link_safety_gaps.py"; spec = importlib.util.spec_from_file_location("script_reply_draft_link_safety_gaps", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def _db():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE reply_drafts (id TEXT, inbound_id TEXT, status TEXT, body TEXT, relationship_context TEXT, citations TEXT, link_reviewed TEXT);")
    c.execute("INSERT INTO reply_drafts VALUES (?,?,?,?,?,?,?)", ("r1", "in1", "pending", "See http://bit.ly/a and https://untrusted.example/x", "https://trusted.example/a", "", "false"))
    c.execute("INSERT INTO reply_drafts VALUES (?,?,?,?,?,?,?)", ("r2", "in2", "sent", "See http://sent.example", "", "", "false"))
    c.commit(); return c

def test_reply_link_safety_report_and_cli(tmp_path, capsys):
    r = build_reply_draft_link_safety_gaps_report_from_db(_db())
    reasons = {f["reason"] for f in r["reply_link_safety_gaps"]}
    assert r["artifact_type"] == "reply_draft_link_safety_gaps"
    assert {"insecure_http_url", "shortened_url", "link_not_in_context", "missing_citation", "unreviewed_link"} <= reasons
    assert all(f["reply_id"] == "r1" for f in r["reply_link_safety_gaps"])
    assert json.loads(format_reply_draft_link_safety_gaps_json(r))["artifact_type"] == r["artifact_type"]
    assert "Reply Draft Link Safety Gaps" in format_reply_draft_link_safety_gaps_text(r)
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0 and capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2

def test_status_filter_and_missing_schema():
    assert build_reply_draft_link_safety_gaps_report_from_db(_db(), statuses=("sent",))["reply_link_safety_gaps"]
    assert build_reply_draft_link_safety_gaps_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
