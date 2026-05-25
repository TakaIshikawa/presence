from __future__ import annotations
import importlib.util, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from evaluation.github_activity_review_latency import build_github_activity_review_latency_report_from_db, format_github_activity_review_latency_json, format_github_activity_review_latency_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"github_activity_review_latency.py"; spec=importlib.util.spec_from_file_location("script_github_activity_review_latency",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE github_activity (id TEXT, repository TEXT, activity_type TEXT, activity_at TEXT); CREATE TABLE content_reviews (activity_id TEXT, reviewed_at TEXT);")
    c.execute("INSERT INTO github_activity VALUES (?,?,?,?)",("a1","org/repo","pr","2026-05-20T00:00:00+00:00")); c.execute("INSERT INTO content_reviews VALUES (?,?)",("a1","2026-05-23T00:00:00+00:00")); c.execute("INSERT INTO github_activity VALUES (?,?,?,?)",("a2","org/repo","commit","2026-05-20T00:00:00+00:00")); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_github_activity_review_latency_report_from_db(_db(),now=datetime(2026,5,25,tzinfo=timezone.utc),max_hours=24); assert r["artifact_type"]=="github_activity_review_latency"; assert len(r["findings"])==2; assert any(f["status"]=="overdue" for f in r["findings"])
    assert json.loads(format_github_activity_review_latency_json(r))["artifact_type"]=="github_activity_review_latency"; assert "GitHub" in format_github_activity_review_latency_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","text","--max-hours","24"])==0; assert capsys.readouterr().out; assert script.main(["--db",str(db),"--limit","0"])==2
def test_missing_schema():
    assert build_github_activity_review_latency_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
