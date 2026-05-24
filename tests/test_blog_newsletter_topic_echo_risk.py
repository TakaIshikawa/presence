from __future__ import annotations
import importlib.util,json,sqlite3
from pathlib import Path
from evaluation.blog_newsletter_topic_echo_risk import build_blog_newsletter_topic_echo_risk_report_from_db,format_blog_newsletter_topic_echo_risk_json
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"blog_newsletter_topic_echo_risk.py"; spec=importlib.util.spec_from_file_location("blog_newsletter_topic_echo_risk_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_topic_echo_and_cli(tmp_path):
 c=sqlite3.connect(":memory:"); c.row_factory=sqlite3.Row; c.executescript("CREATE TABLE blog_posts (id TEXT,title TEXT,topic TEXT,published_at TEXT); CREATE TABLE newsletter_issues (id TEXT,subject TEXT,topic TEXT,sent_at TEXT);")
 c.execute("INSERT INTO blog_posts VALUES ('b1','Python packaging drift','python packaging','2026-01-01T00:00:00+00:00')")
 c.execute("INSERT INTO newsletter_issues VALUES ('n1','Python packaging drift notes','python packaging','2026-01-05T00:00:00+00:00')")
 r=build_blog_newsletter_topic_echo_risk_report_from_db(c,cooldown_days=7,similarity_threshold=.4)
 assert r["findings"][0]["shared_tokens"]==["drift","packaging","python"]; assert json.loads(format_blog_newsletter_topic_echo_risk_json(r))["artifact_type"]=="blog_newsletter_topic_echo_risk"
 p=tmp_path/"db.sqlite"; c.commit()
 with sqlite3.connect(p) as out: c.backup(out)
 assert script.main(["--db",str(p),"--cooldown-days","7"])==0; assert script.main(["--db",str(p),"--similarity-threshold","2"])==2
def test_missing_tables():
 assert set(build_blog_newsletter_topic_echo_risk_report_from_db(sqlite3.connect(":memory:"))["missing_tables"])=={"blog_posts","newsletter_issues"}
