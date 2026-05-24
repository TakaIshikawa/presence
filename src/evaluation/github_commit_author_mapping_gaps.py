"""Report GitHub commits missing author mappings."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='github_commit_author_mapping_gaps'; DEFAULT_LIMIT=50
def _identity(r): return lower(r.get('author_email') or r.get('email') or r.get('author_login') or r.get('login') or r.get('author_name') or 'unknown')
def build_github_commit_author_mapping_gaps_report(rows:list[dict[str,Any]],*,repo:str|None=None,since:str|None=None,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive('limit',limit); since_dt=dt(since); buckets=defaultdict(list)
    for r in rows:
        rp=clean(r.get('repo') or r.get('repo_name') or r.get('repository'))
        ts=dt(r.get('committed_at') or r.get('authored_at') or r.get('created_at'))
        mapped=clean(r.get('person_id') or r.get('mapped_author_id') or r.get('account_id'))
        if repo and rp!=repo: continue
        if since_dt and ts and ts<since_dt: continue
        if mapped: continue
        buckets[(rp,_identity(r))].append((r,ts))
    findings=[]
    for (rp,ident),items in buckets.items():
        dates=[t for _,t in items if t]; first=min(dates).isoformat() if dates else None; last=max(dates).isoformat() if dates else None; sample=[clean(i.get('sha') or i.get('commit_sha')) for i,_ in items[:5]]
        r=items[0][0]; findings.append({'repo':rp,'author_identity':ident,'author_login':r.get('author_login') or r.get('login'),'author_email':r.get('author_email') or r.get('email'),'author_name':r.get('author_name') or r.get('name'),'commit_count':len(items),'first_seen':first,'last_seen':last,'sample_shas':sample})
    findings.sort(key=lambda f:(-f['commit_count'], f['repo'], f['author_identity']))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'repo':repo,'since':since,'limit':limit},'totals':{'commits':len(rows),'findings':len(findings)},'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No GitHub commit author mapping gaps found.',schema_gap=bool(missing_tables or missing_columns))}
def build_github_commit_author_mapping_gaps_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); table=next((t for t in ('github_commits','commits') if t in s),None); rows=[]; mt=[] if table else ['github_commits']
    if table: rows=load_table(conn,table,s[table],{'repo':('repo','repo_name','repository'),'sha':('sha','commit_sha'),'author_login':('author_login','login'),'author_email':('author_email','email'),'author_name':('author_name','name'),'person_id':('person_id','mapped_author_id','account_id'),'committed_at':('committed_at','authored_at','created_at')})
    return build_github_commit_author_mapping_gaps_report(rows,missing_tables=mt,missing_columns={},**kw)
def format_github_commit_author_mapping_gaps_json(r): return json_dumps(r)
def format_github_commit_author_mapping_gaps_text(r):
    lines=['GitHub Commit Author Mapping Gaps',f"Generated: {r['generated_at']}",f"Totals: commits={r['totals']['commits']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines+=['','repo | identity | commits | first_seen | last_seen']
    for f in r['findings']: lines.append(f"{f['repo']} | {f['author_identity']} | {f['commit_count']} | {f['first_seen']} | {f['last_seen']}")
    return '\n'.join(lines)
