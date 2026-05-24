"""Find archived newsletter issues needing metric backfill."""
from __future__ import annotations
from collections import defaultdict
from datetime import timedelta
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="newsletter_archive_metric_backfill_candidates"; DEFAULT_LIMIT=50; DEFAULT_MIN_AGE_HOURS=24; TYPES=("opens","clicks","bounces","unsubscribes")
def build_newsletter_archive_metric_backfill_candidates_report(issue_rows:list[dict[str,Any]],metric_rows:list[dict[str,Any]]|None=None,*,min_age_hours:int=DEFAULT_MIN_AGE_HOURS,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    non_negative('min_age_hours',min_age_hours); positive('limit',limit); gen=now_value(now); present=defaultdict(set)
    for m in metric_rows or []:
        iid=clean(m.get('issue_id') or m.get('newsletter_issue_id'))
        mt=lower(m.get('metric_type') or m.get('name'))
        if mt in TYPES and to_float(m.get('value'),1)!=0: present[iid].add(mt)
        for t in TYPES:
            if m.get(t) not in (None,''): present[iid].add(t)
    findings=[]
    for i,r in enumerate(issue_rows):
        status=lower(r.get('status') or r.get('state'))
        if status not in {'sent','published','archived'}: continue
        ts=dt(r.get('sent_at') or r.get('published_at') or r.get('created_at'))
        age=round((gen-ts).total_seconds()/3600,2) if ts else None
        if age is not None and age<min_age_hours: continue
        iid=clean(r.get('issue_id') or r.get('id'),str(i+1)); missing=[t for t in TYPES if t not in present[iid]]
        if not missing: continue
        audience=to_int(r.get('audience_size') or r.get('recipient_count'))
        score=round(len(missing)*25 + min((age or min_age_hours)/24,30) + min(audience/1000,25),2)
        findings.append({'issue_id':iid,'status':status,'sent_at':ts.isoformat() if ts else None,'age_hours':age,'audience_size':audience,'missing_metrics':missing,'priority_score':score})
    findings.sort(key=lambda f:(-f['priority_score'],-(f['age_hours'] or 0),f['issue_id']))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':gen.isoformat(),'filters':{'min_age_hours':min_age_hours,'limit':limit},'totals':{'issues':len(issue_rows),'candidates':len(findings),'shown':min(len(findings),limit)},'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No newsletter archive metric backfill candidates found.',schema_gap=bool(missing_tables or missing_columns))}
def build_newsletter_archive_metric_backfill_candidates_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[] if 'newsletter_issues' in s else ['newsletter_issues']; mc={}; issues=[]; metrics=[]
    if not mt:
        cols=s['newsletter_issues']; issues=load_table(conn,'newsletter_issues',cols,{'issue_id':('id','issue_id'),'status':('status','state'),'sent_at':('sent_at','published_at'),'published_at':('published_at',),'created_at':('created_at',),'audience_size':('audience_size','recipient_count')})
    for t in ('newsletter_metrics','newsletter_issue_metrics'):
        if t in s:
            metrics+=load_table(conn,t,s[t],{'issue_id':('issue_id','newsletter_issue_id'),'metric_type':('metric_type','name'),'value':('value','metric_value'),'opens':('opens','open_count'),'clicks':('clicks','click_count'),'bounces':('bounces','bounce_count'),'unsubscribes':('unsubscribes','unsubscribe_count')})
    return build_newsletter_archive_metric_backfill_candidates_report(issues,metrics,missing_tables=mt,missing_columns=mc,**kw)
def format_newsletter_archive_metric_backfill_candidates_json(r): return json_dumps(r)
def format_newsletter_archive_metric_backfill_candidates_text(r):
    lines=['Newsletter Archive Metric Backfill Candidates',f"Generated: {r['generated_at']}",f"Totals: issues={r['totals']['issues']} candidates={r['totals']['candidates']} shown={r['totals']['shown']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines += ['','issue_id | priority | age_hours | audience | missing_metrics']
    for f in r['findings']: lines.append(f"{f['issue_id']} | {f['priority_score']} | {f['age_hours']} | {f['audience_size']} | {', '.join(f['missing_metrics'])}")
    return '\n'.join(lines)
