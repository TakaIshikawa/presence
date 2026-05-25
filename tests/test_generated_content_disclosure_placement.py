from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.generated_content_disclosure_placement import build_generated_content_disclosure_placement_report_from_db, format_generated_content_disclosure_placement_json, format_generated_content_disclosure_placement_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"generated_content_disclosure_placement.py"; spec=importlib.util.spec_from_file_location("script_generated_content_disclosure_placement",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE generated_content (id TEXT, channel TEXT, content TEXT);")
    c.execute("INSERT INTO generated_content VALUES (?,?,?)",("c1","blog","Intro. "+"x"*300+" Created with AI.")); c.execute("INSERT INTO generated_content VALUES (?,?,?)",("c2","x","Short post without label")); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_generated_content_disclosure_placement_report_from_db(_db(),allowed_offset=100); assert r["artifact_type"]=="generated_content_disclosure_placement"; assert r["summary"]["missing"]==1 and r["summary"]["late"]==1
    assert json.loads(format_generated_content_disclosure_placement_json(r))["artifact_type"]=="generated_content_disclosure_placement"; assert "Disclosure" in format_generated_content_disclosure_placement_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","text","--allowed-offset","100"])==0; assert capsys.readouterr().out; assert script.main(["--db",str(db),"--limit","0"])==2
def test_missing_schema():
    assert build_generated_content_disclosure_placement_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
