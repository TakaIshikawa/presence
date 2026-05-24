from __future__ import annotations
import importlib.util,json,sqlite3
from datetime import datetime,timezone
from pathlib import Path
from evaluation.github_activity_author_response_time import build_github_activity_author_response_time_report_from_db,format_github_activity_author_response_time_json
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"github_activity_author_response_time.py"; spec=importlib.util.spec_from_file_location("github_activity_author_response_time_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_response_time_and_cli(tmp_path):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE github_activity (id TEXT,author TEXT,repository TEXT,occurred_at TEXT); CREATE TABLE reply_queue (activity_id TEXT,created_at TEXT);")
 c.executemany("INSERT INTO github_activity VALUES (?,?,?,?)",[("a1","ann","repo","2026-01-01T00:00:00+00:00"),("a2","ann","repo","2026-01-01T00:00:00+00:00")])
 c.execute("INSERT INTO reply_queue VALUES ('a1','2026-01-02T12:00:00+00:00')")
 r=build_github_activity_author_response_time_report_from_db(c,sla_hours=24,now=datetime(2026,1,3,tzinfo=timezone.utc))
 assert {f["issue_type"] for f in r["findings"]}=={"missing_response","sla_breach"}; assert json.loads(format_github_activity_author_response_time_json(r))["artifact_type"]=="github_activity_author_response_time"
 p=tmp_path/"db.sqlite"; c.commit()
 with sqlite3.connect(p) as out: c.backup(out)
 assert script.main(["--db",str(p),"--sla-hours","24"])==0; assert script.main(["--db",str(p),"--sla-hours","0"])==2
def test_missing_table():
 assert build_github_activity_author_response_time_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["github_activity"]
