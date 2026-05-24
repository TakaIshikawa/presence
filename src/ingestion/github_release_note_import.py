"""Import GitHub release note records."""
from __future__ import annotations
from typing import Any
from ._batch_file_import import *
ARTIFACT_TYPE='github_release_note_import'
def _repo(v): return clean(v).lower()
def parse_github_release_note_records(path):
    out=[]
    for r in read_records(path):
        repo=_repo(r.get('repo') or r.get('repo_name')); tag=clean(r.get('tag_name') or r.get('tag'))
        if not repo or not tag: raise ValueError('repo and tag_name are required')
        out.append({'repo':repo,'tag_name':tag,'title':clean(r.get('title') or r.get('name')),'body':clean(r.get('body')),'published_at':clean(r.get('published_at')),'author_login':clean(r.get('author_login')),'html_url':clean(r.get('html_url') or r.get('url')),'prerelease':truthy(r.get('prerelease')),'draft':truthy(r.get('draft'))})
    return out
def import_github_release_notes(db_or_conn:Any,path,*,dry_run:bool=False):
    rows=parse_github_release_note_records(path); c=conn(db_or_conn)
    if not dry_run:
        c.execute('CREATE TABLE IF NOT EXISTS github_release_notes (repo TEXT, tag_name TEXT, title TEXT, body TEXT, published_at TEXT, author_login TEXT, html_url TEXT, prerelease INTEGER, draft INTEGER, PRIMARY KEY(repo,tag_name))')
        c.executemany('INSERT OR REPLACE INTO github_release_notes VALUES (:repo,:tag_name,:title,:body,:published_at,:author_login,:html_url,:prerelease,:draft)',rows); c.commit()
    return summary(ARTIFACT_TYPE,len(rows),0 if dry_run else len(rows),dry_run)
def format_github_release_note_import_json(r): return dumps(r)
def format_github_release_note_import_text(r): return f"GitHub Release Note Import: parsed={r['parsed']} upserted={r['upserted']} dry_run={int(r['dry_run'])}"
