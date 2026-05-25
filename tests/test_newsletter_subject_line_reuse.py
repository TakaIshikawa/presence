from __future__ import annotations
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_subject_line_reuse import build_newsletter_subject_line_reuse_report_from_db, format_newsletter_subject_line_reuse_json, format_newsletter_subject_line_reuse_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_subject_line_reuse.py"; spec=importlib.util.spec_from_file_location("newsletter_subject_line_reuse_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE newsletter_issues (id TEXT, subject TEXT, sent_at TEXT)")
    c.executemany("INSERT INTO newsletter_issues VALUES (?,?,?)",[("n1","AI Launch Checklist!","2026-05-01T00:00:00+00:00"),("n2","ai launch checklist","2026-05-03T00:00:00+00:00"),("n3","Completely different","2026-05-04T00:00:00+00:00")]); c.commit(); return c
def test_reuse_report_cli_and_formats(tmp_path,capsys):
    r=build_newsletter_subject_line_reuse_report_from_db(_db(),lookback_days=60,similarity_threshold=.7,now=datetime(2026,5,25,tzinfo=timezone.utc))
    assert r["groups"][0]["reuse_count"]==2
    assert "Newsletter Subject" in format_newsletter_subject_line_reuse_text(r)
    assert json.loads(format_newsletter_subject_line_reuse_json(r))["artifact_type"]=="newsletter_subject_line_reuse"
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close()
    assert script.main(["--db",str(db),"--format","text","--similarity-threshold",".7"])==0
    assert "launch" in capsys.readouterr().out
    assert script.main(["--db",str(db),"--lookback-days","0"])==2
