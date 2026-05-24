"""Import newsletter subscriber tag snapshots."""
from __future__ import annotations
from typing import Any
from ._batch_file_import import *
ARTIFACT_TYPE='newsletter_subscriber_tag_import'
def parse_newsletter_subscriber_tag_records(path): 
    out=[]
    for r in read_records(path):
        sid=clean(r.get('subscriber_id')); email=clean(r.get('email')); tag=clean(r.get('tag')); observed=clean(r.get('observed_at'))
        if not (sid or email) or not tag or not observed: raise ValueError('subscriber_id or email, tag, and observed_at are required')
        out.append({'subscriber_id':sid,'email':email,'tag':tag,'observed_at':observed,'source':clean(r.get('source')),'status':clean(r.get('status'))})
    return out
def import_newsletter_subscriber_tags(db_or_conn:Any,path,*,dry_run:bool=False):
    rows=parse_newsletter_subscriber_tag_records(path); c=conn(db_or_conn)
    if not dry_run:
        c.execute('CREATE TABLE IF NOT EXISTS newsletter_subscriber_tags (subscriber_id TEXT, email TEXT, tag TEXT NOT NULL, observed_at TEXT NOT NULL, source TEXT, status TEXT, PRIMARY KEY(subscriber_id,email,tag,observed_at))')
        c.executemany('INSERT OR REPLACE INTO newsletter_subscriber_tags VALUES (:subscriber_id,:email,:tag,:observed_at,:source,:status)',rows); c.commit()
    return summary(ARTIFACT_TYPE,len(rows),0 if dry_run else len(rows),dry_run)
def format_newsletter_subscriber_tag_import_json(r): return dumps(r)
def format_newsletter_subscriber_tag_import_text(r): return f"Newsletter Subscriber Tag Import: parsed={r['parsed']} upserted={r['upserted']} dry_run={int(r['dry_run'])}"
