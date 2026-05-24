"""Detect repeated newsletter subject lines."""
from __future__ import annotations
import re
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='newsletter_subject_line_reuse'; DEFAULT_LOOKBACK_DAYS=90; DEFAULT_SIMILARITY=0.8; DEFAULT_LIMIT=50
def _norm(v): return ' '.join(re.findall(r'[a-z0-9]+', clean(v).lower()))
def build_newsletter_subject_line_reuse_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,similarity_threshold:float=DEFAULT_SIMILARITY,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive('lookback_days',lookback_days); bounded_share('similarity_threshold',similarity_threshold); positive('limit',limit)
    groups=[]; used=set()
    ordered=sorted(rows,key=lambda r:(dt(r.get('send_date') or r.get('sent_at') or r.get('created_at')) or now_value(now), str(r.get('issue_id') or r.get('id'))))
    for i,row in enumerate(ordered):
        if i in used: continue
        subj=clean(row.get('subject') or row.get('subject_line') or row.get('title')); n=_norm(subj); toks=tokens(n); members=[]
        for j,other in enumerate(ordered[i:], start=i):
            osub=clean(other.get('subject') or other.get('subject_line') or other.get('title')); on=_norm(osub); sim=1.0 if n and n==on else jaccard(toks,tokens(on))
            a=dt(row.get('send_date') or row.get('sent_at') or row.get('created_at')); b=dt(other.get('send_date') or other.get('sent_at') or other.get('created_at')); gap=abs((b-a).days) if a and b else 0
            if sim>=similarity_threshold and gap<=lookback_days:
                members.append({'issue_id':other.get('issue_id') or other.get('id'),'subject':osub,'send_date':(b.isoformat() if b else None),'similarity':sim}); used.add(j)
        if len(members)>1:
            dates=[dt(m['send_date']) for m in members if m.get('send_date')]; dates=[d for d in dates if d]
            groups.append({'normalized_subject':n,'reuse_count':len(members),'issue_ids':[m['issue_id'] for m in members],'send_dates':[m['send_date'] for m in members],'most_recent_reuse_age_days':(now_value(now)-max(dates)).days if dates else None,'subjects':[m['subject'] for m in members],'severity':len(members)*100+(max(m['similarity'] for m in members)*10)})
    groups.sort(key=lambda g:(-g['reuse_count'], g['most_recent_reuse_age_days'] if g['most_recent_reuse_age_days'] is not None else 999999, g['normalized_subject']))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'lookback_days':lookback_days,'similarity_threshold':similarity_threshold,'limit':limit},'totals':{'issues':len(rows),'reuse_groups':len(groups)},'findings':groups[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(groups,'No newsletter subject line reuse found.',schema_gap=bool(missing_tables or missing_columns))}
def build_newsletter_subject_line_reuse_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[] if 'newsletter_issues' in s else ['newsletter_issues']; rows=[]
    if not mt: rows=load_table(conn,'newsletter_issues',s['newsletter_issues'],{'issue_id':('issue_id','id'),'subject':('subject','subject_line','title'),'send_date':('send_date','sent_at','published_at','created_at')})
    return build_newsletter_subject_line_reuse_report(rows,missing_tables=mt,missing_columns={},**kw)
def format_newsletter_subject_line_reuse_json(r): return json_dumps(r)
def format_newsletter_subject_line_reuse_text(r):
    lines=['Newsletter Subject Line Reuse',f"Generated: {r['generated_at']}",f"Totals: issues={r['totals']['issues']} reuse_groups={r['totals']['reuse_groups']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines+=['','normalized_subject | reuse_count | issue_ids']
    for f in r['findings']: lines.append(f"{f['normalized_subject']} | {f['reuse_count']} | {', '.join(map(str,f['issue_ids']))}")
    return '\n'.join(lines)
