from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.blog_draft_reading_time_outliers import build_blog_draft_reading_time_outliers_report_from_db,format_blog_draft_reading_time_outliers_json,format_blog_draft_reading_time_outliers_text
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"blog_draft_reading_time_outliers.py"; spec=importlib.util.spec_from_file_location("blog_draft_reading_time_outliers_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def _db():
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.execute("CREATE TABLE generated_content (id TEXT, content_type TEXT, title TEXT, slug TEXT, content TEXT, created_at TEXT)")
 c.executemany("INSERT INTO generated_content VALUES (?,?,?,?,?,?)",[("short","blog_draft","Short","short","tiny draft","2026-05-20T00:00:00+00:00"),("ok","blog_draft","Ok","ok","word "*450,"2026-05-20T00:00:00+00:00"),("long","blog_draft","Long","long","word "*1600,"2026-05-20T00:00:00+00:00"),("x","x_post","X","x","word "*1600,"2026-05-20T00:00:00+00:00")]); c.commit(); return c
def test_builder_flags_underlong_overlong_and_counts_acceptable():
 r=build_blog_draft_reading_time_outliers_report_from_db(_db(),days=30,min_minutes=2,max_minutes=5,limit=10)
 assert r["artifact_type"]=="blog_draft_reading_time_outliers"; assert r["totals"]["draft_count"]==3; assert r["totals"]["acceptable_count"]==1
 assert {f["reason"] for f in r["findings"]}=={"underlong","overlong"}; assert r["findings"][0]["draft_id"]=="long"
 assert json.loads(format_blog_draft_reading_time_outliers_json(r))["artifact_type"]=="blog_draft_reading_time_outliers"; assert "blog_draft_reading_time_outliers" in format_blog_draft_reading_time_outliers_text(r)
def test_missing_schema_and_cli_validation(tmp_path,capsys):
 assert build_blog_draft_reading_time_outliers_report_from_db(sqlite3.connect(":memory:"))["missing_tables"]==["generated_content"]
 bad=sqlite3.connect(":memory:"); bad.execute("CREATE TABLE generated_content (id TEXT, content_type TEXT)")
 assert "generated_content" in build_blog_draft_reading_time_outliers_report_from_db(bad)["missing_columns"]
 db=tmp_path/"db.sqlite"; out=sqlite3.connect(db); _db().backup(out); out.close()
 assert script.main(["--db",str(db),"--format","json","--min-minutes","2","--max-minutes","5"])==0; assert json.loads(capsys.readouterr().out)["findings"]
 assert script.main(["--db",str(db),"--min-minutes","0"])!=0
