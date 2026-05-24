from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.blog_draft_meta_description_gaps import build_blog_draft_meta_description_gaps_report_from_db, format_blog_draft_meta_description_gaps_json, format_blog_draft_meta_description_gaps_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"blog_draft_meta_description_gaps.py"; spec=importlib.util.spec_from_file_location("script_blog_draft_meta_description_gaps",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE blog_drafts (id TEXT, title TEXT, meta_description TEXT, metadata TEXT, body TEXT);")
    c.execute("INSERT INTO blog_drafts VALUES (?,?,?,?,?)",("d1","One","Learn more",None,"")); c.execute("INSERT INTO blog_drafts VALUES (?,?,?,?,?)",("d2","Two","Learn more",None,"")); c.execute("INSERT INTO blog_drafts VALUES (?,?,?,?,?)",("d3","Three",None,'{\"meta_description\":\"Short\"}',"")); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_blog_draft_meta_description_gaps_report_from_db(_db(),min_length=20,max_length=80); assert r["artifact_type"]=="blog_draft_meta_description_gaps"; assert r["findings"]; assert r["summary"]["by_reason"]["duplicate"]==2
    assert json.loads(format_blog_draft_meta_description_gaps_json(r))["artifact_type"]=="blog_draft_meta_description_gaps"; assert "Meta" in format_blog_draft_meta_description_gaps_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","text","--min-length","20","--max-length","80"])==0; assert capsys.readouterr().out; assert script.main(["--db",str(db),"--limit","0"])==2
def test_missing_schema():
    assert build_blog_draft_meta_description_gaps_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
