"""Track content feedback reopen rates."""
from __future__ import annotations
from collections import defaultdict
from statistics import median
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='content_feedback_reopen_rate'; DEFAULT_LIMIT=50; DEFAULT_WINDOW_DAYS=30; DEFAULT_MIN_RESOLVED=1
def build_content_feedback_reopen_rate_report(rows:list[dict[str,Any]],*,window_days:int=DEFAULT_WINDOW_DAYS,min_resolved:int=DEFAULT_MIN_RESOLVED,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive('window_days',window_days); positive('min_resolved',min_resolved); positive('limit',limit); gen=now_value(now); groups=defaultdict(lambda:{'resolved':0,'reopened':0,'times':[]})
    items=defaultdict(list)
    for r in rows: items[clean(r.get('feedback_id') or r.get('id'))].append(r)
    findings=[]
    for fid,evs in items.items():
        evs.sort(key=lambda r: dt(r.get('occurred_at') or r.get('created_at')) or gen)
        reviewer=clean(evs[-1].get('reviewer') or evs[-1].get('reviewer_id'),'unknown'); ctype=clean(evs[-1].get('content_type'),'unknown'); reason=clean(evs[-1].get('resolution_reason'),'unknown'); key=(reviewer,ctype,reason); resolved_at=None
        for e in evs:
            st=lower(e.get('event_type') or e.get('status'))
            ts=dt(e.get('occurred_at') or e.get('created_at'))
            if st in {'resolved','closed','approved'}: groups[key]['resolved']+=1; resolved_at=ts
            if st in {'reopened','open'} and resolved_at and ts:
                groups[key]['reopened']+=1; groups[key]['times'].append(round((ts-resolved_at).total_seconds()/3600,2)); findings.append({'feedback_id':fid,'reviewer':reviewer,'content_type':ctype,'resolution_reason':reason,'time_to_reopen_hours':groups[key]['times'][-1],'reopened_at':ts.isoformat()})
    breakdown=[]
    for (rev,ct,rea),v in groups.items():
        if v['resolved']<min_resolved: continue
        rate=round(v['reopened']/v['resolved'],4); breakdown.append({'reviewer':rev,'content_type':ct,'resolution_reason':rea,'resolved_count':v['resolved'],'reopen_count':v['reopened'],'reopen_rate':rate,'median_time_to_reopen_hours':median(v['times']) if v['times'] else None})
    findings.sort(key=lambda f:(-f['time_to_reopen_hours'],f['reviewer'],f['feedback_id']))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':gen.isoformat(),'filters':{'window_days':window_days,'min_resolved':min_resolved,'limit':limit},'totals':{'feedback_items':len(items),'findings':len(findings)},'reviewer_breakdown':sorted(breakdown,key=lambda b:(-b['reopen_rate'],b['reviewer'])),'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No content feedback reopen events found.',schema_gap=bool(missing_tables or missing_columns))}
def build_content_feedback_reopen_rate_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[] if ('content_feedback' in s or 'content_feedback_events' in s) else ['content_feedback']; rows=[]; mc={}
    for t in ('content_feedback','content_feedback_events'):
        if t in s: rows+=load_table(conn,t,s[t],{'feedback_id':('feedback_id','id'),'event_type':('event_type','status'),'reviewer':('reviewer','reviewer_id'),'content_type':('content_type',),'resolution_reason':('resolution_reason','reason'),'occurred_at':('occurred_at','created_at')})
    return build_content_feedback_reopen_rate_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_content_feedback_reopen_rate_json(r): return json_dumps(r)
def format_content_feedback_reopen_rate_text(r):
    lines=['Content Feedback Reopen Rate',f"Generated: {r['generated_at']}",f"Totals: feedback_items={r['totals']['feedback_items']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines += ['','reviewer | content_type | reopen_rate | reopen_count']
    for b in r['reviewer_breakdown']: lines.append(f"{b['reviewer']} | {b['content_type']} | {b['reopen_rate']} | {b['reopen_count']}")
    return '\n'.join(lines)
