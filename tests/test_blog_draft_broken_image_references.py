from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.blog_draft_broken_image_references import build_blog_draft_broken_image_references_report_from_db, format_blog_draft_broken_image_references_json, format_blog_draft_broken_image_references_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"blog_draft_broken_image_references.py"; spec=importlib.util.spec_from_file_location("script_blog_draft_broken_image_references",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE generated_content (id TEXT, content_type TEXT, content TEXT, created_at TEXT)")
    c.execute("INSERT INTO generated_content VALUES (?,?,?,?)",("c1","blog",'![ok](ok.png) ![missing](missing.png) ![]() <img src=""> <img src="ftp://x/y.png">',"2026-05-01"))
    c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    (tmp_path/"ok.png").write_text("x")
    r=build_blog_draft_broken_image_references_report_from_db(_db(),base_dir=tmp_path)
    issues={i["issue_type"] for i in r["broken_image_references"]}
    assert {"missing_local_file","empty_reference","unsupported_scheme"} <= issues
    assert json.loads(format_blog_draft_broken_image_references_json(r))["artifact_type"]=="blog_draft_broken_image_references"; assert "Broken Image" in format_blog_draft_broken_image_references_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","text","--base-dir",str(tmp_path)])==0; assert capsys.readouterr().out; assert script.main(["--db",str(db),"--limit","0"])==2
def test_missing_schema_warning():
    r=build_blog_draft_broken_image_references_report_from_db(sqlite3.connect(":memory:"))
    assert r["broken_image_references"]==[] and r["warning"] and r["missing_tables"]==["generated_content"]
