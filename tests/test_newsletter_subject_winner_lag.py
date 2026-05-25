from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.newsletter_subject_winner_lag import build_newsletter_subject_winner_lag_report_from_db, format_newsletter_subject_winner_lag_json, format_newsletter_subject_winner_lag_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_subject_winner_lag.py"; spec=importlib.util.spec_from_file_location("script_newsletter_subject_winner_lag",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
    c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE newsletter_subject_candidates (issue_id TEXT, subject TEXT, score REAL, selected INTEGER, selected_at TEXT); CREATE TABLE newsletter_issues (id TEXT, selected_subject TEXT, send_at TEXT);")
    c.execute("INSERT INTO newsletter_subject_candidates VALUES (?,?,?,?,?)",("i1","Winner",9,0,None)); c.execute("INSERT INTO newsletter_subject_candidates VALUES (?,?,?,?,?)",("i1","Used",5,1,"2026-05-24T20:00:00+00:00")); c.execute("INSERT INTO newsletter_issues VALUES (?,?,?)",("i1","Used","2026-05-25T00:00:00+00:00")); c.commit(); return c
def test_report_db_formatters_and_cli(tmp_path,capsys):
    r=build_newsletter_subject_winner_lag_report_from_db(_db()); assert r["artifact_type"]=="newsletter_subject_winner_lag"; assert r["summary"]["winner_not_used"]==1; assert r["findings"][0]["winning_subject"]=="Winner"
    assert json.loads(format_newsletter_subject_winner_lag_json(r))["artifact_type"]=="newsletter_subject_winner_lag"; assert "Newsletter" in format_newsletter_subject_winner_lag_text(r)
    db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close(); assert script.main(["--db",str(db),"--format","text","--max-lag-hours","2"])==0; assert capsys.readouterr().out; assert script.main(["--db",str(db),"--limit","0"])==2
def test_missing_schema():
    assert build_newsletter_subject_winner_lag_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]
