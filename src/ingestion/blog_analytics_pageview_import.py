"""Import blog analytics pageview snapshots."""
from __future__ import annotations
from typing import Any
from ._batch_file_import import *
ARTIFACT_TYPE='blog_analytics_pageview_import'
def parse_blog_analytics_pageview_records(path):
    out=[]
    for r in read_records(path):
        pathv=canonical_path(r.get('path') or r.get('url')); date=clean(r.get('date'))
        if not pathv or not date: raise ValueError('path/url and date are required')
        out.append({'canonical_path':pathv,'date':date,'pageviews':intval(r.get('pageviews')),'unique_visitors':intval(r.get('unique_visitors')),'avg_time_seconds':floatval(r.get('avg_time_seconds')),'referrer':clean(r.get('referrer')),'source':clean(r.get('source'))})
    return out
def import_blog_analytics_pageviews(db_or_conn:Any,path,*,dry_run:bool=False):
    rows=parse_blog_analytics_pageview_records(path); c=conn(db_or_conn)
    if not dry_run:
        c.execute('CREATE TABLE IF NOT EXISTS blog_analytics_pageviews (canonical_path TEXT, date TEXT, pageviews INTEGER, unique_visitors INTEGER, avg_time_seconds REAL, referrer TEXT, source TEXT, PRIMARY KEY(canonical_path,date))')
        c.executemany('INSERT OR REPLACE INTO blog_analytics_pageviews VALUES (:canonical_path,:date,:pageviews,:unique_visitors,:avg_time_seconds,:referrer,:source)',rows); c.commit()
    return summary(ARTIFACT_TYPE,len(rows),0 if dry_run else len(rows),dry_run)
def format_blog_analytics_pageview_import_json(r): return dumps(r)
def format_blog_analytics_pageview_import_text(r): return f"Blog Analytics Pageview Import: parsed={r['parsed']} upserted={r['upserted']} dry_run={int(r['dry_run'])}"
