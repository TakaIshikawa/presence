"""Pipeline Run Artifact Retention Gaps."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from typing import Any
from urllib.parse import urlparse
import re
from ._report_utils import clean,connection,dt,expr,json_dumps,lower,now_iso,schema,to_float,to_int

ARTIFACT_TYPE='pipeline_run_artifact_retention_gaps'; DEFAULT_LIMIT=50
def build_pipeline_run_artifact_retention_gaps_report(runs:list[dict[str,Any]],artifacts:list[dict[str,Any]],*,max_age_days:int=30,max_size_bytes:int=100000000,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
 if max_age_days<0 or max_size_bytes<=0 or limit<=0: raise ValueError('invalid threshold')
 gen=dt(now) if now else datetime.now(timezone.utc); by=defaultdict(list)
 for a in artifacts: by[clean(a.get('run_id') or a.get('pipeline_run_id'))].append(a)
 findings=[]
 for r in runs:
  rid=clean(r.get('run_id') or r.get('id')); stage=clean(r.get('stage'),'unknown')
  arts=by.get(rid,[])
  if not arts: findings.append({'run_id':rid,'stage':stage,'reason':'missing_artifact'}); continue
  for a in arts:
   reason=None; created=dt(a.get('created_at') or a.get('expires_at')); size=to_int(a.get('size_bytes')) or 0
   if lower(a.get('status'))=='expired' or (created and (gen-created).days>max_age_days): reason='expired'
   if size>max_size_bytes: reason='oversized'
   if not clean(a.get('path') or a.get('url')): reason='unlinked'
   if reason: findings.append({'run_id':rid,'artifact_id':a.get('artifact_id') or a.get('id'),'stage':stage,'reason':reason,'size_bytes':size})
 run_ids={clean(r.get('run_id') or r.get('id')) for r in runs}
 for a in artifacts:
  if clean(a.get('run_id') or a.get('pipeline_run_id')) not in run_ids: findings.append({'run_id':a.get('run_id') or a.get('pipeline_run_id'),'artifact_id':a.get('artifact_id') or a.get('id'),'stage':'unknown','reason':'orphan_artifact','size_bytes':to_int(a.get('size_bytes')) or 0})
 findings.sort(key=lambda f:(f['reason'],f.get('stage',''),str(f.get('run_id'))))
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'max_age_days':max_age_days,'max_size_bytes':max_size_bytes,'limit':limit},'totals':{'runs':len(runs),'artifacts':len(artifacts),'findings':len(findings),'shown_findings':len(findings[:limit])},'stage_breakdown':dict(sorted(Counter(f['stage'] for f in findings).items())),'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No pipeline run artifact retention gaps found.' if not findings else None}}
def build_pipeline_run_artifact_retention_gaps_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 if 'pipeline_runs' not in s: return build_pipeline_run_artifact_retention_gaps_report([],[],missing_tables=['pipeline_runs'],**kw)
 rc=s['pipeline_runs']; runs=[dict(r) for r in conn.execute(f"SELECT {expr(rc,'id','run_id',out='run_id')}, {expr(rc,'stage',default='NULL',out='stage')} FROM pipeline_runs ORDER BY rowid")]
 t=next((x for x in ('pipeline_artifacts','publish_artifacts') if x in s),None); arts=[]; missing=[] if t else ['pipeline_artifacts|publish_artifacts']
 if t:
  c=s[t]; arts=[dict(r) for r in conn.execute(f"SELECT {expr(c,'id','artifact_id',default='NULL',out='artifact_id')}, {expr(c,'run_id','pipeline_run_id',default='NULL',out='run_id')}, {expr(c,'path','url',default='NULL',out='path')}, {expr(c,'status',default='NULL',out='status')}, {expr(c,'created_at','expires_at',default='NULL',out='created_at')}, {expr(c,'size_bytes',default='0',out='size_bytes')} FROM {t} ORDER BY rowid")]
 return build_pipeline_run_artifact_retention_gaps_report(runs,arts,missing_tables=missing,**kw)
def format_pipeline_run_artifact_retention_gaps_json(r): return json_dumps(r)
def format_pipeline_run_artifact_retention_gaps_text(r):
 lines=['Pipeline Run Artifact Retention Gaps',f"Generated: {r['generated_at']}",f"Totals: runs={r['totals']['runs']} findings={r['totals']['findings']}"]
 if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','run_id | artifact_id | stage | reason | size_bytes']
 for f in r['findings']: lines.append(f"{f.get('run_id')} | {f.get('artifact_id','-')} | {f.get('stage')} | {f.get('reason')} | {f.get('size_bytes','-')}")
 return '\n'.join(lines)
