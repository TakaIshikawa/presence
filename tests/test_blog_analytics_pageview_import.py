from __future__ import annotations
import importlib.util, json, sqlite3
from pathlib import Path
from ingestion.blog_analytics_pageview_import import import_blog_analytics_pageviews
SCRIPT=Path(__file__).resolve().parent.parent/'scripts'/'import_blog_analytics_pageviews.py'; spec=importlib.util.spec_from_file_location('import_blog_analytics_pageviews_script',SCRIPT); script=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(script)
def test_pageview_numeric_url_upsert_dry_run_cli(tmp_path,capsys):
    p=tmp_path/'pv.csv'; p.write_text('url,date,pageviews,unique_visitors,avg_time_seconds,referrer,source\nhttps://Ex.test/blog/a?x=1,2026-05-01,10,7,12.5,,ga\n')
    c=sqlite3.connect(':memory:'); import_blog_analytics_pageviews(c,p)
    assert tuple(c.execute('SELECT canonical_path,pageviews,avg_time_seconds FROM blog_analytics_pageviews').fetchone())==('/blog/a',10,12.5)
    p.write_text('path,date,pageviews\n/blog/a,2026-05-01,20\n'); import_blog_analytics_pageviews(c,p)
    assert c.execute('SELECT pageviews FROM blog_analytics_pageviews').fetchone()[0]==20
    assert import_blog_analytics_pageviews(c,p,dry_run=True)['upserted']==0
    db=tmp_path/'db.sqlite'; assert script.main(['--db',str(db),'--input',str(p),'--format','json'])==0
    assert json.loads(capsys.readouterr().out)['upserted']==1
