"""GitHub Activity Author Response Time."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from typing import Any
import re
from ._report_utils import clean,connection,dt,expr,json_dumps,lower,now_iso,schema,to_int

ARTIFACT_TYPE='github_activity_author_response_time'; DEFAULT_LIMIT=50
def _bucket(h): return 'under_1h' if h<=1 else 'under_24h' if h<=24 else 'under_72h' if h<=72 else 'over_72h'
def build_github_activity_author_response_time_report(activities:list[dict[str,Any]],responses:list[dict[str,Any]],*,sla_hours:int=24,window_days:int=30,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
 if sla_hours<=0 or window_days<=0 or limit<=0: raise ValueError('positive values required')
 gen=dt(now) if now else datetime.now(timezone.utc); start=gen-timedelta(days=window_days)
 resp=defaultdict(list)
 for r in responses:
  key=clean(r.get('activity_id') or r.get('github_activity_id')); at=dt(r.get('responded_at') or r.get('created_at'))
  if key and at: resp[key].append(at)
 findings=[]; lat=[]; author=defaultdict(list); repo=defaultdict(list)
 for a in activities:
  at=dt(a.get('occurred_at') or a.get('created_at'))
  if not at or at<start: continue
  aid=clean(a.get('activity_id') or a.get('id')); first=min(resp.get(aid,[]),default=None); auth=clean(a.get('author'),'unknown'); repository=clean(a.get('repository'),'unknown')
  if first:
   h=round((first-at).total_seconds()/3600,2); lat.append(h); author[auth].append(h); repo[repository].append(h)
   if h>sla_hours: findings.append({'activity_id':aid,'author':auth,'repository':repository,'latency_hours':h,'reason':'sla_breach'})
  else: findings.append({'activity_id':aid,'author':auth,'repository':repository,'latency_hours':None,'reason':'missing_response'})
 findings.sort(key=lambda f:(f['reason'],-(f['latency_hours'] or 999999),f['repository'],f['author']))
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(gen),'filters':{'sla_hours':sla_hours,'window_days':window_days,'limit':limit},'totals':{'activities':len(activities),'responses':len(responses),'responded':len(lat),'findings':len(findings),'shown_findings':len(findings[:limit])},'latency_buckets':dict(sorted(Counter(_bucket(x) for x in lat).items())),'author_breakdown':[{'author':k,'responses':len(v),'avg_latency_hours':round(sum(v)/len(v),2)} for k,v in sorted(author.items())],'repo_breakdown':[{'repository':k,'responses':len(v),'avg_latency_hours':round(sum(v)/len(v),2)} for k,v in sorted(repo.items())],'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No GitHub activity author response time findings found.' if not findings else None}}
def build_github_activity_author_response_time_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 if 'github_activity' not in s: return build_github_activity_author_response_time_report([],[],missing_tables=['github_activity'],**kw)
 c=s['github_activity']; acts=[dict(r) for r in conn.execute(f"SELECT {expr(c,'id','activity_id',out='activity_id')}, {expr(c,'author',default='NULL',out='author')}, {expr(c,'repository','repo',default='NULL',out='repository')}, {expr(c,'occurred_at','created_at',default='NULL',out='occurred_at')} FROM github_activity ORDER BY rowid")]
 responses=[]; missing=[]
 for t in ('reply_queue','proactive_actions'):
  if t in s:
   rc=s[t]; responses += [dict(r) for r in conn.execute(f"SELECT {expr(rc,'activity_id','github_activity_id',default='NULL',out='activity_id')}, {expr(rc,'responded_at','created_at',default='NULL',out='responded_at')} FROM {t} ORDER BY rowid")]
 if not responses: missing=['reply_queue|proactive_actions']
 return build_github_activity_author_response_time_report(acts,responses,missing_tables=missing,**kw)
def format_github_activity_author_response_time_json(r): return json_dumps(r)
def format_github_activity_author_response_time_text(r):
 lines=['GitHub Activity Author Response Time',f"Generated: {r['generated_at']}",f"Totals: activities={r['totals']['activities']} findings={r['totals']['findings']}"]
 if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','activity_id | repository | author | latency_hours | reason']
 for f in r['findings']: lines.append(f"{f['activity_id']} | {f['repository']} | {f['author']} | {f['latency_hours'] if f['latency_hours'] is not None else '-'} | {f['reason']}")
 return '\n'.join(lines)
