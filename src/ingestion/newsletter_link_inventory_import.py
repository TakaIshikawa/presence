"""Import newsletter link inventory records."""
from __future__ import annotations
from typing import Any
from ._batch_file_import import *
ARTIFACT_TYPE='newsletter_link_inventory_import'
def parse_newsletter_link_inventory_records(path):
    out=[]
    for r in read_records(path):
        issue=clean(r.get('issue_id')); url=normalize_url(r.get('url')); observed=clean(r.get('observed_at'))
        if not issue or not url or not observed: raise ValueError('issue_id, url, and observed_at are required')
        out.append({'issue_id':issue,'url':url,'link_text':clean(r.get('link_text')),'section':clean(r.get('section')),'position':intval(r.get('position')),'utm_campaign':clean(r.get('utm_campaign')),'observed_at':observed})
    return out
def import_newsletter_link_inventory(db_or_conn:Any,path,*,dry_run:bool=False):
    rows=parse_newsletter_link_inventory_records(path); c=conn(db_or_conn)
    if not dry_run:
        c.execute('CREATE TABLE IF NOT EXISTS newsletter_link_inventory (issue_id TEXT, url TEXT, link_text TEXT, section TEXT, position INTEGER, utm_campaign TEXT, observed_at TEXT, PRIMARY KEY(issue_id,url))')
        c.executemany('INSERT OR REPLACE INTO newsletter_link_inventory VALUES (:issue_id,:url,:link_text,:section,:position,:utm_campaign,:observed_at)',rows); c.commit()
    return summary(ARTIFACT_TYPE,len(rows),0 if dry_run else len(rows),dry_run)
def format_newsletter_link_inventory_import_json(r): return dumps(r)
def format_newsletter_link_inventory_import_text(r): return f"Newsletter Link Inventory Import: parsed={r['parsed']} upserted={r['upserted']} dry_run={int(r['dry_run'])}"
