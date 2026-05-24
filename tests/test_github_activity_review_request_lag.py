from __future__ import annotations
import importlib.util, sqlite3
from pathlib import Path
from datetime import datetime, timezone
from evaluation.github_activity_review_request_lag import build_github_activity_review_request_lag_report_from_db, format_github_activity_review_request_lag_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"github_activity_review_request_lag.py"; spec=importlib.util.spec_from_file_location("github_activity_review_request_lag_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_review_request_lag_and_cli(tmp_path,capsys):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE github_activity (repository TEXT, pr_number TEXT, event_type TEXT, created_at TEXT, requested_reviewer TEXT, actor TEXT)")
 c.execute("INSERT INTO github_activity VALUES ('o/r','1','review_requested','2026-05-20T00:00:00+00:00','bob','ann')")
 c.execute("INSERT INTO github_activity VALUES ('o/r','2','review_requested','2026-05-20T00:00:00+00:00','sam','ann')")
 c.execute("INSERT INTO github_activity VALUES ('o/r','2','commented','2026-05-20T01:00:00+00:00',NULL,'sam')")
 r=build_github_activity_review_request_lag_report_from_db(c,sla_hours=24,now=datetime(2026,5,25,tzinfo=timezone.utc))
 assert len(r["findings"])==1 and r["findings"][0]["pr_number"]=="1"
 assert "Review Request Lag" in format_github_activity_review_request_lag_text(r)
 c.commit(); path=tmp_path/"db.sqlite"
 with sqlite3.connect(path) as out: c.backup(out)
 assert script.main(["--db",str(path),"--format","text","--sla-hours","24"])==0; assert "Review Request" in capsys.readouterr().out
 assert script.main(["--db",str(path),"--sla-hours","0"])==2
def test_review_request_schema_gap(): assert build_github_activity_review_request_lag_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["github_activity"]
