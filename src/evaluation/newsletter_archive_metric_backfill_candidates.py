"""Newsletter Archive Metric Backfill Candidates."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
from ._report_utils import clean,connection,dt,expr,json_dumps,loads_list,loads_obj,lower,now_iso,schema,to_float,to_int

ARTIFACT_TYPE='newsletter_archive_metric_backfill_candidates'; DEFAULT_LIMIT=50; DEFAULT_MIN_AGE_HOURS=24; METRICS=('opens','clicks','bounces','unsubscribes')
def build_newsletter_archive_metric_backfill_candidates_report(issues:list[dict[str,Any]],metrics:list[dict[str,Any]],*,min_age_hours:int=DEFAULT_MIN_AGE_HOURS,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
 if min_age_hours<0: raise ValueError('min_age_hours must be non-negative')
 if limit<=0: raise ValueError('limit must be positive')
 gen=dt(now) if now else datetime.now(timezone.utc); have=defaultdict(set)
 for m in metrics:
  iid=clean(m.get('issue_id') or m.get('newsletter_issue_id'))
  typ=lower(m.get('metric_type') or m.get('name'))
  for x in METRICS:
   if typ==x or (to_float(m.get(x)) is not None): have[iid].add(x)
 findings=[]
 for i,r in enumerate(issues):
  status=lower(r.get('status') or r.get('state'))
  sent=dt(r.get('sent_at') or r.get('published_at'))
  if status not in {'sent','published','archived'} or not sent: continue
  age=(gen-sent).total_seconds()/3600
  missing=[m for m in METRICS if m not in have[clean(r.get('issue_id') or r.get('id'))]]
  if age>=min_age_hours and missing:
   audience=to_int(r.get('audience_size') or r.get('recipient_count')) or 0; score=round(age/24+audience/100+len(missing)*10,2)
   findings.append({'issue_id':r.get('issue_id') or r.get('id'),'title':clean(r.get('title')) or None,'sent_at':sent.isoformat(),'age_hours':round(age,2),'audience_size':audience,'missing_metrics':missing,'priority_score':score,'_i':i})
 findings.sort(key=lambda f:(-f['priority_score'],f['issue_id']))
 shown=findings[:limit]
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(gen),'filters':{'min_age_hours':min_age_hours,'limit':limit},'totals':{'issues':len(issues),'candidates':len(findings),'shown_findings':len(shown)},'findings':shown,'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No newsletter archive metric backfill candidates found.' if not findings else None}}
def build_newsletter_archive_metric_backfill_candidates_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 if 'newsletter_issues' not in s: return build_newsletter_archive_metric_backfill_candidates_report([],[],missing_tables=['newsletter_issues'],**kw)
 ic=s['newsletter_issues']; issues=[dict(r) for r in conn.execute(f"SELECT {expr(ic,'id','issue_id',out='issue_id')}, {expr(ic,'title',default='NULL',out='title')}, {expr(ic,'status','state',default='NULL',out='status')}, {expr(ic,'sent_at','published_at',default='NULL',out='sent_at')}, {expr(ic,'audience_size','recipient_count',default='0',out='audience_size')} FROM newsletter_issues ORDER BY rowid")]
 mt=next((t for t in ('newsletter_metrics','newsletter_issue_metrics') if t in s),None); metrics=[]; missing=[] if mt else ['newsletter_metrics|newsletter_issue_metrics']
 if mt:
  c=s[mt]; metrics=[dict(r) for r in conn.execute(f"SELECT {expr(c,'issue_id','newsletter_issue_id',default='NULL',out='issue_id')}, {expr(c,'metric_type','name',default='NULL',out='metric_type')}, {expr(c,'opens',default='NULL',out='opens')}, {expr(c,'clicks',default='NULL',out='clicks')}, {expr(c,'bounces',default='NULL',out='bounces')}, {expr(c,'unsubscribes',default='NULL',out='unsubscribes')} FROM {mt} ORDER BY rowid")]
 return build_newsletter_archive_metric_backfill_candidates_report(issues,metrics,missing_tables=missing,**kw)
def format_newsletter_archive_metric_backfill_candidates_json(r): return json_dumps(r)
def format_newsletter_archive_metric_backfill_candidates_text(r):
 lines=['Newsletter Archive Metric Backfill Candidates',f"Generated: {r['generated_at']}",f"Totals: issues={r['totals']['issues']} candidates={r['totals']['candidates']}"]
 if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','issue_id | age_hours | audience_size | priority_score | missing_metrics']
 for f in r['findings']: lines.append(f"{f['issue_id']} | {f['age_hours']} | {f['audience_size']} | {f['priority_score']} | {', '.join(f['missing_metrics'])}")
 return '\n'.join(lines)
