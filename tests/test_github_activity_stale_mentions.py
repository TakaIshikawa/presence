from __future__ import annotations
from datetime import datetime, timezone
import importlib.util, json, sqlite3
from pathlib import Path
from evaluation.github_activity_stale_mentions import build_github_activity_stale_mentions_report_from_db,format_github_activity_stale_mentions_json,format_github_activity_stale_mentions_text
NOW=datetime(2026,5,24,tzinfo=timezone.utc)
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"github_activity_stale_mentions.py"; spec=importlib.util.spec_from_file_location("github_activity_stale_mentions_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_stale_mentions_and_cli(tmp_path,capsys):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row
 c.executescript("CREATE TABLE github_activity (id INTEGER, repo TEXT, number INTEGER, mention_type TEXT, mentioned_at TEXT, owner TEXT); CREATE TABLE content_ideas (github_activity_id INTEGER, content_id INTEGER, status TEXT);")
 c.execute("INSERT INTO github_activity VALUES (1,'o/r',7,'planning','2026-05-01T00:00:00+00:00','amy')"); c.execute("INSERT INTO github_activity VALUES (2,'o/r',8,'planning','2026-05-23T00:00:00+00:00','bob')"); c.execute("INSERT INTO content_ideas VALUES (1,42,'open')")
 r=build_github_activity_stale_mentions_report_from_db(c,stale_days=7,now=NOW)
 assert r["findings"][0]["linked_content_id"]=="42"; assert r["summary"]["by_repo"]=={"o/r":1}; assert json.loads(format_github_activity_stale_mentions_json(r))["artifact_type"]=="github_activity_stale_mentions"; assert "GitHub Activity" in format_github_activity_stale_mentions_text(r)
 c.commit(); path=tmp_path/"db.sqlite"
 with sqlite3.connect(path) as out: c.backup(out)
 assert script.main(["--db",str(path),"--format","text"])==0; assert "GitHub Activity" in capsys.readouterr().out; assert script.main(["--db",str(path),"--stale-days","0"])==2
def test_missing_table():
 assert build_github_activity_stale_mentions_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["github_activity"]
