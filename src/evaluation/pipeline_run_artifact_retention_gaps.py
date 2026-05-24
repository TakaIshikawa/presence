"""Find pipeline run artifact retention gaps."""
from __future__ import annotations
from collections import defaultdict,Counter
from datetime import timedelta
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='pipeline_run_artifact_retention_gaps'; DEFAULT_LIMIT=50; DEFAULT_MAX_AGE_DAYS=30; DEFAULT_MAX_SIZE_BYTES=100000000
def build_pipeline_run_artifact_retention_gaps_report(run_rows:list[dict[str,Any]],artifact_rows:list[dict[str,Any]]|None=None,*,max_age_days:int=DEFAULT_MAX_AGE_DAYS,max_size_bytes:int=DEFAULT_MAX_SIZE_BYTES,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive('max_age_days',max_age_days); positive('max_size_bytes',max_size_bytes); positive('limit',limit); gen=now_value(now); arts=defaultdict(list)
    for a in artifact_rows or []: arts[clean(a.get('run_id') or a.get('pipeline_run_id'))].append(a)
    findings=[]; bystage=Counter()
    for i,r in enumerate(run_rows):
        rid=clean(r.get('run_id') or r.get('id'),str(i+1)); stage=clean(r.get('stage'),'unknown'); expected=clean(r.get('expected_artifacts') or r.get('expected_artifact_type') or 'artifact')
        if not arts.get(rid): findings.append({'run_id':rid,'stage':stage,'reason':'missing_artifact','severity':60}); bystage[stage]+=1
        for a in arts.get(rid,[]):
            reason=None; sev=0; created=dt(a.get('created_at') or r.get('created_at'))
            if lower(a.get('status')) in {'expired','deleted'} or (created and gen-created>timedelta(days=max_age_days)): reason='expired_artifact'; sev=40
            if to_int(a.get('size_bytes'))>max_size_bytes: reason='oversized_artifact'; sev=30
            if not clean(a.get('run_id') or a.get('pipeline_run_id')): reason='orphan_artifact'; sev=50
            if not clean(a.get('url') or a.get('path') or a.get('artifact_path')): reason='unlinked_artifact'; sev=max(sev,45)
            if reason: findings.append({'run_id':rid,'stage':stage,'artifact_id':a.get('artifact_id') or a.get('id'),'reason':reason,'size_bytes':to_int(a.get('size_bytes')),'severity':sev}); bystage[stage]+=1
    findings.sort(key=lambda f:(-f['severity'],f['stage'],f['run_id']))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':gen.isoformat(),'filters':{'max_age_days':max_age_days,'max_size_bytes':max_size_bytes,'limit':limit},'totals':{'runs':len(run_rows),'artifacts':len(artifact_rows or []),'findings':len(findings)},'stage_summary':dict(sorted(bystage.items())),'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No pipeline run artifact retention gaps found.',schema_gap=bool(missing_tables or missing_columns))}
def build_pipeline_run_artifact_retention_gaps_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[] if 'pipeline_runs' in s else ['pipeline_runs']; mc={}; runs=[]; arts=[]
    if not mt: runs=load_table(conn,'pipeline_runs',s['pipeline_runs'],{'run_id':('id','run_id'),'stage':('stage',),'status':('status',),'created_at':('created_at','started_at'),'expected_artifacts':('expected_artifacts','expected_artifact_type')})
    for t in ('pipeline_artifacts','publish_artifacts'):
        if t in s: arts+=load_table(conn,t,s[t],{'artifact_id':('id','artifact_id'),'run_id':('run_id','pipeline_run_id'),'stage':('stage',),'status':('status',),'size_bytes':('size_bytes',),'url':('url','path','artifact_path'),'created_at':('created_at',)})
    return build_pipeline_run_artifact_retention_gaps_report(runs,arts,missing_tables=mt,missing_columns=mc,**kw)
def format_pipeline_run_artifact_retention_gaps_json(r): return json_dumps(r)
def format_pipeline_run_artifact_retention_gaps_text(r):
    lines=['Pipeline Run Artifact Retention Gaps',f"Generated: {r['generated_at']}",f"Totals: runs={r['totals']['runs']} artifacts={r['totals']['artifacts']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines += ['','run_id | stage | reason | severity']
    for f in r['findings']: lines.append(f"{f['run_id']} | {f['stage']} | {f['reason']} | {f['severity']}")
    return '\n'.join(lines)
