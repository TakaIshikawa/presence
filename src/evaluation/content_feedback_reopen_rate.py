"""Content Feedback Reopen Rate."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from statistics import median
from typing import Any
from urllib.parse import urlparse
from ._report_utils import clean,connection,dt,expr,json_dumps,loads_list,loads_obj,lower,now_iso,schema,to_float,to_int

ARTIFACT_TYPE='content_feedback_reopen_rate'; DEFAULT_LIMIT=50
def build_content_feedback_reopen_rate_report(rows:list[dict[str,Any]],*,window_days:int=30,min_resolved:int=1,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
 if window_days<=0 or min_resolved<=0 or limit<=0: raise ValueError('positive values required')
 gen=dt(now) if now else datetime.now(timezone.utc); start=gen-timedelta(days=window_days); by=defaultdict(list)
 for r in rows:
  at=dt(r.get('occurred_at') or r.get('created_at'))
  if at and at>=start: by[clean(r.get('feedback_id') or r.get('id'))].append((at,lower(r.get('status') or r.get('event_type')),r))
 resolved=0; reopened=[]
 for fid,events in by.items():
  events.sort(key=lambda x:x[0]); last_res=None; resrow={}
  for at,status,row in events:
   if status in {'resolved','closed'}: resolved+=1; last_res=at; resrow=row
   if status in {'reopened','open'} and last_res:
    reopened.append({'feedback_id':fid,'reviewer':clean(row.get('reviewer') or resrow.get('reviewer'),'unknown'),'content_type':clean(row.get('content_type') or resrow.get('content_type'),'unknown'),'resolution_reason':clean(resrow.get('resolution_reason'),'unknown'),'time_to_reopen_hours':round((at-last_res).total_seconds()/3600,2)}); last_res=None
 rate=round(len(reopened)/max(resolved,1),4); findings=reopened if resolved>=min_resolved and reopened else []
 rev=Counter(x['reviewer'] for x in reopened); ctype=Counter(x['content_type'] for x in reopened); reason=Counter(x['resolution_reason'] for x in reopened)
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(gen),'filters':{'window_days':window_days,'min_resolved':min_resolved,'limit':limit},'totals':{'resolved':resolved,'reopened':len(reopened),'reopen_rate':rate,'median_time_to_reopen_hours':median([x['time_to_reopen_hours'] for x in reopened]) if reopened else None,'findings':len(findings),'shown_findings':len(findings[:limit])},'reviewer_breakdown':dict(sorted(rev.items())),'content_type_breakdown':dict(sorted(ctype.items())),'resolution_reason_breakdown':dict(sorted(reason.items())),'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No content feedback reopen rate findings found.' if not findings else None}}
def build_content_feedback_reopen_rate_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn); t='content_feedback_events' if 'content_feedback_events' in s else 'content_feedback' if 'content_feedback' in s else None
 if not t: return build_content_feedback_reopen_rate_report([],missing_tables=['content_feedback'],**kw)
 c=s[t]; rows=[dict(r) for r in conn.execute(f"SELECT {expr(c,'feedback_id','id',default='NULL',out='feedback_id')}, {expr(c,'status','event_type',default='NULL',out='status')}, {expr(c,'reviewer',default='NULL',out='reviewer')}, {expr(c,'content_type',default='NULL',out='content_type')}, {expr(c,'resolution_reason',default='NULL',out='resolution_reason')}, {expr(c,'occurred_at','created_at',default='NULL',out='occurred_at')} FROM {t} ORDER BY rowid")]
 return build_content_feedback_reopen_rate_report(rows,**kw)
def format_content_feedback_reopen_rate_json(r): return json_dumps(r)
def format_content_feedback_reopen_rate_text(r):
 lines=['Content Feedback Reopen Rate',f"Generated: {r['generated_at']}",f"Totals: resolved={r['totals']['resolved']} reopened={r['totals']['reopened']} rate={r['totals']['reopen_rate']}"]
 if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','feedback_id | reviewer | content_type | resolution_reason | time_to_reopen_hours']
 for f in r['findings']: lines.append(f"{f['feedback_id']} | {f['reviewer']} | {f['content_type']} | {f['resolution_reason']} | {f['time_to_reopen_hours']}")
 return '\n'.join(lines)
