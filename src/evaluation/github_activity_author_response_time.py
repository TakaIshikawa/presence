"""Measure GitHub activity first response time by author and repository."""
from __future__ import annotations
from collections import defaultdict,Counter
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='github_activity_author_response_time'; DEFAULT_LIMIT=50; DEFAULT_SLA_HOURS=24; DEFAULT_WINDOW_DAYS=30
def _bucket(h):
    if h is None: return 'missing'
    if h<=4: return '0-4h'
    if h<=24: return '4-24h'
    if h<=72: return '24-72h'
    return '72h+'
def build_github_activity_author_response_time_report(activity_rows:list[dict[str,Any]],response_rows:list[dict[str,Any]]|None=None,*,sla_hours:int=DEFAULT_SLA_HOURS,window_days:int=DEFAULT_WINDOW_DAYS,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive('sla_hours',sla_hours); positive('window_days',window_days); positive('limit',limit); responses=defaultdict(list); gen=now_value(now)
    for r in response_rows or []: responses[clean(r.get('activity_id') or r.get('github_activity_id'))].append(r)
    author=defaultdict(lambda:{'count':0,'overdue':0,'buckets':Counter()}); repo=defaultdict(lambda:{'count':0,'overdue':0,'buckets':Counter()}); findings=[]
    for i,a in enumerate(activity_rows):
        aid=clean(a.get('activity_id') or a.get('id'),str(i+1)); ats=dt(a.get('occurred_at') or a.get('created_at')); auth=clean(a.get('author'),'unknown'); rp=clean(a.get('repository') or a.get('repo'),'unknown')
        candidates=[dt(r.get('responded_at') or r.get('created_at')) for r in responses.get(aid,[])]; candidates=[x for x in candidates if x and (not ats or x>=ats)]
        first=min(candidates) if candidates else None; hours=round((first-ats).total_seconds()/3600,2) if first and ats else None; b=_bucket(hours)
        for g,k in ((author,auth),(repo,rp)): g[k]['count']+=1; g[k]['buckets'][b]+=1; g[k]['overdue']+=1 if hours is None or hours>sla_hours else 0
        if hours is None or hours>sla_hours: findings.append({'activity_id':aid,'author':auth,'repository':rp,'occurred_at':ats.isoformat() if ats else None,'first_response_at':first.isoformat() if first else None,'latency_hours':hours,'reason':'missing_response' if hours is None else 'sla_breach','severity':100 if hours is None else round(hours-sla_hours,2)})
    findings.sort(key=lambda f:(-f['severity'],f['repository'],f['author'],f['activity_id']))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':gen.isoformat(),'filters':{'sla_hours':sla_hours,'window_days':window_days,'limit':limit},'totals':{'activities':len(activity_rows),'responses':len(response_rows or []),'findings':len(findings)},'author_breakdown':[{'author':k,'activity_count':v['count'],'overdue_count':v['overdue'],'latency_buckets':dict(sorted(v['buckets'].items()))} for k,v in sorted(author.items())],'repo_breakdown':[{'repository':k,'activity_count':v['count'],'overdue_count':v['overdue'],'latency_buckets':dict(sorted(v['buckets'].items()))} for k,v in sorted(repo.items())],'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No GitHub activity response time findings found.',schema_gap=bool(missing_tables or missing_columns))}
def build_github_activity_author_response_time_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[] if 'github_activity' in s else ['github_activity']; mc={}; acts=[]; res=[]
    if 'github_activity' in s: acts=load_table(conn,'github_activity',s['github_activity'],{'activity_id':('id','activity_id'),'author':('author','actor'),'repository':('repository','repo'),'occurred_at':('occurred_at','created_at')})
    for t in ('reply_queue','proactive_actions'):
        if t in s: res+=load_table(conn,t,s[t],{'activity_id':('github_activity_id','activity_id'),'responded_at':('responded_at','created_at'),'status':('status',)})
    return build_github_activity_author_response_time_report(acts,res,missing_tables=mt,missing_columns=mc,**kw)
def format_github_activity_author_response_time_json(r): return json_dumps(r)
def format_github_activity_author_response_time_text(r):
    lines=['GitHub Activity Author Response Time',f"Generated: {r['generated_at']}",f"Totals: activities={r['totals']['activities']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines += ['','activity_id | repository | author | latency_hours | reason']
    for f in r['findings']: lines.append(f"{f['activity_id']} | {f['repository']} | {f['author']} | {f['latency_hours']} | {f['reason']}")
    return '\n'.join(lines)
