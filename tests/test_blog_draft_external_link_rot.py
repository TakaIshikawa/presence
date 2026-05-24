from __future__ import annotations
import importlib.util, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.blog_draft_external_link_rot import build_blog_draft_external_link_rot_report_from_db, format_blog_draft_external_link_rot_json, format_blog_draft_external_link_rot_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"blog_draft_external_link_rot.py"; spec=importlib.util.spec_from_file_location("script_blog_draft_external_link_rot",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE blog_drafts (id TEXT, title TEXT, body TEXT); CREATE TABLE link_snapshots (url TEXT, status TEXT, http_code INTEGER, checked_at TEXT);")
    c.execute("INSERT INTO blog_drafts VALUES (?,?,?)",("d1","Draft","See https://dead.example.org/page for details")); c.execute("INSERT INTO link_snapshots VALUES (?,?,?,?)",("https://dead.example.org/page","broken",404,"2026-05-24T00:00:00+00:00")); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_blog_draft_external_link_rot_report_from_db(_db(),now=datetime(2026,5,25,tzinfo=timezone.utc)); assert r["artifact_type"]=="blog_draft_external_link_rot"; assert r["broken_links"]; assert r["broken_links"][0]["host"]=="dead.example.org"
    assert json.loads(format_blog_draft_external_link_rot_json(r))["artifact_type"]=="blog_draft_external_link_rot"; assert "Blog" in format_blog_draft_external_link_rot_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","text","--stale-days","1"])==0; assert capsys.readouterr().out; assert script.main(["--db",str(db),"--limit","0"])==2
def test_missing_schema():
    assert build_blog_draft_external_link_rot_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
