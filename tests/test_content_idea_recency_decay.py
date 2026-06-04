from __future__ import annotations
import importlib.util,sqlite3
from datetime import datetime,timezone
from pathlib import Path
from evaluation.content_idea_recency_decay import build_content_idea_recency_decay_report
NOW=datetime(2026,6,1,tzinfo=timezone.utc)
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"content_idea_recency_decay.py"; spec=importlib.util.spec_from_file_location("script_content_idea_recency_decay",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_classifies_warning_stale_and_no_evidence():
 rows=[{"id":"w","status":"open","newest_evidence_at":"2026-05-01T00:00:00+00:00","evidence_count":1},{"id":"s","status":"open","newest_evidence_at":"2026-03-01T00:00:00+00:00","evidence_count":1},{"id":"n","status":"open","evidence_count":0}]
 r=build_content_idea_recency_decay_report(rows,warning_after_days=20,stale_after_days=60,now=NOW)
 assert {i["idea_id"]:i["severity"] for i in r["decays"]}=={"s":"stale","n":"stale","w":"warning"}
def test_excludes_completed_unless_status_included():
 rows=[{"id":"done","status":"published","evidence_count":0}]
 assert build_content_idea_recency_decay_report(rows,now=NOW)["decays"]==[]
 assert build_content_idea_recency_decay_report(rows,statuses=["published"],now=NOW)["decays"]
def test_cli(tmp_path,capsys):
 db=tmp_path/"db.sqlite"; c=sqlite3.connect(db); c.execute("CREATE TABLE content_ideas (id TEXT,title TEXT,status TEXT,evidence_count INTEGER)"); c.execute("INSERT INTO content_ideas VALUES (?,?,?,?)",("i1","Idea","open",0)); c.commit(); c.close()
 assert script.main(["--db",str(db),"--format","text"])==0; assert capsys.readouterr().out
