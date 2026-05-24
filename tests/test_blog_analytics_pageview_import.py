from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from ingestion.blog_analytics_pageview_import import parse_blog_analytics_pageviews, upsert_blog_analytics_pageviews
SCRIPT=Path(__file__).resolve().parent.parent/"scripts"/"import_blog_analytics_pageviews.py"; spec=importlib.util.spec_from_file_location("import_blog_analytics_pageviews_script",SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_pageview_import_cli(tmp_path,capsys):
    raw="url,date,pageviews,unique_visitors,avg_time_seconds\nhttps://example.com/a?x=1,2026-05-01,10,7,12.5\n"; rows=parse_blog_analytics_pageviews(raw); assert rows[0]["canonical_path"]=="/a"
    c=sqlite3.connect(":memory:"); upsert_blog_analytics_pageviews(c,rows); upsert_blog_analytics_pageviews(c,[{**rows[0],"pageviews":20}]); assert c.execute("SELECT pageviews FROM blog_analytics_pageviews").fetchone()[0]==20
    p=tmp_path/"p.csv"; p.write_text(raw); db=tmp_path/"db.sqlite"; assert script.main(["--db",str(db),"--input",str(p),"--dry-run","--format","json"])==0
    assert json.loads(capsys.readouterr().out)["parsed_count"]==1
