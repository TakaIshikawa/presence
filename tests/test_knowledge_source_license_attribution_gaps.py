from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.knowledge_source_license_attribution_gaps import build_knowledge_source_license_attribution_gaps_report_from_db, format_knowledge_source_license_attribution_gaps_json, format_knowledge_source_license_attribution_gaps_text
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "knowledge_source_license_attribution_gaps.py"; spec = importlib.util.spec_from_file_location("script_knowledge_source_license_attribution_gaps", SCRIPT); script = importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)

def _db():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE knowledge_sources (id TEXT, title TEXT, license TEXT, source_url TEXT, author TEXT, attribution TEXT); CREATE TABLE content_knowledge_links (content_id TEXT, source_id TEXT);")
    c.execute("INSERT INTO knowledge_sources VALUES (?,?,?,?,?,?)", ("s1", "One", "", "", "", ""))
    c.execute("INSERT INTO knowledge_sources VALUES (?,?,?,?,?,?)", ("s2", "Two", "All rights reserved", "https://e.test", "A", "A"))
    c.execute("INSERT INTO content_knowledge_links VALUES (?,?)", ("c1", "s1")); c.execute("INSERT INTO content_knowledge_links VALUES (?,?)", ("c2", "s2"))
    c.commit(); return c

def test_license_attribution_report_and_cli(tmp_path, capsys):
    r = build_knowledge_source_license_attribution_gaps_report_from_db(_db())
    reasons = {f["reason"] for f in r["source_license_attribution_gaps"]}
    assert r["artifact_type"] == "knowledge_source_license_attribution_gaps"
    assert {"missing_license", "unknown_license_on_reused_source", "missing_source_url", "missing_author_attribution", "incompatible_license_for_reuse"} <= reasons
    assert any(f["usage_count"] == 1 for f in r["source_license_attribution_gaps"])
    assert json.loads(format_knowledge_source_license_attribution_gaps_json(r))["artifact_type"] == r["artifact_type"]
    assert "Knowledge Source License Attribution Gaps" in format_knowledge_source_license_attribution_gaps_text(r)
    db = tmp_path / "db.sqlite"; out = sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db", str(db), "--format", "text"]) == 0 and capsys.readouterr().out
    assert script.main(["--db", str(db), "--limit", "0"]) == 2

def test_missing_schema():
    assert build_knowledge_source_license_attribution_gaps_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
