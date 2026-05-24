from __future__ import annotations
import importlib.util,json,sqlite3
from datetime import datetime,timezone
from pathlib import Path
from evaluation.newsletter_archive_metric_backfill_candidates import build_newsletter_archive_metric_backfill_candidates_report_from_db,format_newsletter_archive_metric_backfill_candidates_json
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_archive_metric_backfill_candidates.py"; spec=importlib.util.spec_from_file_location("newsletter_archive_metric_backfill_candidates_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_backfill_candidates_and_cli(tmp_path):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE newsletter_issues (id TEXT,status TEXT,sent_at TEXT,audience_size INTEGER); CREATE TABLE newsletter_metrics (issue_id TEXT,opens INTEGER,clicks INTEGER,bounces INTEGER,unsubscribes INTEGER);")
 c.executemany("INSERT INTO newsletter_issues VALUES (?,?,?,?)",[("i1","sent","2026-01-01T00:00:00+00:00",5000),("i2","draft","2026-01-01T00:00:00+00:00",50)])
 c.execute("INSERT INTO newsletter_metrics VALUES ('i1',10,NULL,NULL,NULL)")
 r=build_newsletter_archive_metric_backfill_candidates_report_from_db(c,min_age_hours=1,now=datetime(2026,1,3,tzinfo=timezone.utc))
 assert r["findings"][0]["missing_metrics"]==["clicks","bounces","unsubscribes"]; assert json.loads(format_newsletter_archive_metric_backfill_candidates_json(r))["artifact_type"]=="newsletter_archive_metric_backfill_candidates"
 p=tmp_path/"db.sqlite"; c.commit()
 with sqlite3.connect(p) as out: c.backup(out)
 assert script.main(["--db",str(p),"--min-age-hours","1"])==0; assert script.main(["--db",str(p),"--limit","0"])==2
def test_missing_table():
 assert build_newsletter_archive_metric_backfill_candidates_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["newsletter_issues"]
