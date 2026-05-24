"""Publication Attempt Retry Reason Drift."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
from ._report_utils import clean,connection,dt,expr,json_dumps,loads_list,loads_obj,lower,now_iso,schema,to_float,to_int

ARTIFACT_TYPE='publication_attempt_retry_reason_drift'; DEFAULT_LIMIT=50
def build_publication_attempt_retry_reason_drift_report(rows:list[dict[str,Any]],*,baseline_days:int=14,current_days:int=7,min_delta:float=.25,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
 if baseline_days<=0 or current_days<=0: raise ValueError('window days must be positive')
 if min_delta<0: raise ValueError('min_delta must be non-negative')
 if limit<=0: raise ValueError('limit must be positive')
 gen=dt(now) if now else datetime.now(timezone.utc); cur_start=gen-timedelta(days=current_days); base_start=cur_start-timedelta(days=baseline_days)
 counts=defaultdict(lambda:{'baseline':Counter(),'current':Counter()})
 for r in rows:
  at=dt(r.get('attempted_at') or r.get('created_at'))
  if not at: continue
  key=(clean(r.get('platform'),'unknown').lower(),clean(r.get('retry_reason') or r.get('error_category'),'unknown').lower())
  bucket='current' if at>=cur_start else 'baseline' if at>=base_start else None
  if bucket: counts[key][bucket][key[1]]+=1
 findings=[]
 totals={'baseline':sum(sum(v['baseline'].values()) for v in counts.values()),'current':sum(sum(v['current'].values()) for v in counts.values())}
 for (p,reason),v in counts.items():
  b=sum(v['baseline'].values()); c=sum(v['current'].values()); bs=b/max(totals['baseline'],1); cs=c/max(totals['current'],1); delta=round(cs-bs,4)
  if abs(delta)>=min_delta: findings.append({'platform':p,'reason':reason,'baseline_count':b,'current_count':c,'baseline_share':round(bs,4),'current_share':round(cs,4),'delta':delta})
 findings.sort(key=lambda f:(-abs(f['delta']),f['platform'],f['reason']))
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(gen),'filters':{'baseline_days':baseline_days,'current_days':current_days,'min_delta':min_delta,'limit':limit},'totals':{'baseline':totals['baseline'],'current':totals['current'],'findings':len(findings),'shown_findings':len(findings[:limit])},'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No publication retry reason drift found.' if not findings else None}}
def build_publication_attempt_retry_reason_drift_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn); t=next((x for x in ('publication_retries','publication_attempts') if x in s),None)
 if not t: return build_publication_attempt_retry_reason_drift_report([],missing_tables=['publication_attempts|publication_retries'],**kw)
 c=s[t]; rows=[dict(r) for r in conn.execute(f"SELECT {expr(c,'attempted_at','created_at',default='NULL',out='attempted_at')}, {expr(c,'platform',default='NULL',out='platform')}, {expr(c,'retry_reason',default='NULL',out='retry_reason')}, {expr(c,'error_category',default='NULL',out='error_category')}, {expr(c,'status',default='NULL',out='status')} FROM {t} ORDER BY rowid")]
 return build_publication_attempt_retry_reason_drift_report(rows,**kw)
def format_publication_attempt_retry_reason_drift_json(r): return json_dumps(r)
def format_publication_attempt_retry_reason_drift_text(r):
 lines=['Publication Attempt Retry Reason Drift',f"Generated: {r['generated_at']}",f"Totals: baseline={r['totals']['baseline']} current={r['totals']['current']} findings={r['totals']['findings']}"]
 if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','platform | reason | baseline_count | current_count | delta']
 for f in r['findings']: lines.append(f"{f['platform']} | {f['reason']} | {f['baseline_count']} | {f['current_count']} | {f['delta']}")
 return '\n'.join(lines)
