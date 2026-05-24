from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.newsletter_signup_source_quality import build_newsletter_signup_source_quality_report_from_db,format_newsletter_signup_source_quality_json,format_newsletter_signup_source_quality_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"newsletter_signup_source_quality.py"; spec=importlib.util.spec_from_file_location("newsletter_signup_source_quality_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_source_quality_and_cli(tmp_path,capsys):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE newsletter_subscribers (id INTEGER, source TEXT, campaign TEXT, consented_at TEXT, created_at TEXT, email TEXT);")
 c.executemany("INSERT INTO newsletter_subscribers VALUES (?,?,?,?,?,?)",[(1,"ad",None,None,"2026-01-02","a@mailinator.com"),(2,"ad",None,None,"2026-01-03","b@mailinator.com"),(3,"ad","spring","2026-01-01","2026-01-04","c@example.com"),(4,"organic","may","2026-01-01","2026-01-05","x@example.org")])
 r=build_newsletter_signup_source_quality_report_from_db(c,min_subscribers=2,burst_threshold=2)
 assert r["artifact_type"]=="newsletter_signup_source_quality"; assert r["source_breakdown"][0]["source"]=="ad"; assert {"missing_consent","missing_campaign","disposable_domain","domain_burst"} <= {f["issue_type"] for f in r["findings"]}; assert json.loads(format_newsletter_signup_source_quality_json(r))["artifact_type"]=="newsletter_signup_source_quality"; assert "Newsletter Signup Source" in format_newsletter_signup_source_quality_text(r)
 c.commit(); path=tmp_path/"db.sqlite"
 with sqlite3.connect(path) as out: c.backup(out)
 assert script.main(["--db",str(path),"--format","text","--min-subscribers","2","--burst-threshold","2"])==0; assert "Newsletter Signup Source" in capsys.readouterr().out; assert script.main(["--db",str(path),"--limit","0"])==2
def test_missing_table():
 assert build_newsletter_signup_source_quality_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["newsletter_subscribers"]
