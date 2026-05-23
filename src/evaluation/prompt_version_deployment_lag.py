"""Detect approved prompt versions with delayed first usage."""
from ._focused_report_helpers import *
ARTIFACT_TYPE="prompt_version_deployment_lag"; DEFAULT_MAX_LAG_HOURS=24; DEFAULT_WINDOW_DAYS=90
def build_prompt_version_deployment_lag_report(rows, usage_rows=None, *, max_lag_hours=DEFAULT_MAX_LAG_HOURS, window_days=DEFAULT_WINDOW_DAYS, prompt_name=None, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 _nonneg('max_lag_hours',max_lag_hours); _positive('window_days',window_days); _positive('limit',limit); gen=_now(now); cutoff=gen-timedelta(days=window_days); usage_rows=usage_rows or []; first={}
 for u in usage_rows:
  key=(u.get('prompt_name') or u.get('name'), str(u.get('version') or u.get('prompt_version') or u.get('prompt_version_id') or '')); dt=_dt(u.get('created_at') or u.get('started_at'))
  if key not in first or (dt and (first[key] is None or dt<first[key])): first[key]=dt
 findings=[]; scoped=[]
 for r in rows:
  name=r.get('prompt_name') or r.get('name'); ver=str(r.get('version') or r.get('prompt_version') or r.get('id') or '')
  if prompt_name and name!=prompt_name: continue
  approved=_dt(r.get('approved_at') or r.get('created_at'));
  if approved and approved<cutoff: continue
  scoped.append(r); fu=first.get((name,ver)) or first.get((name,str(r.get('id') or ''))); lag=((fu or gen)-approved).total_seconds()/3600 if approved else None
  if fu is None: reason='unused_approved_prompt'
  elif lag is not None and lag>max_lag_hours: reason='deployment_lag_exceeded'
  else: continue
  findings.append({'reason':reason,'prompt_name':name,'version':ver,'approved_at':approved.isoformat() if approved else None,'first_used_at':fu.isoformat() if fu else None,'lag_hours':round(lag,2) if lag is not None else None,'detail':reason.replace('_',' ')})
 return _finish(ARTIFACT_TYPE,gen,{'max_lag_hours':max_lag_hours,'window_days':window_days,'prompt_name':prompt_name,'limit':limit},len(scoped),findings,limit,missing_tables,missing_columns)
def build_prompt_version_deployment_lag_report_from_db(db_or_conn, **kwargs):
 conn=_conn(db_or_conn); s=_schema(conn); p=s.get('prompt_versions')
 if p is None: return build_prompt_version_deployment_lag_report([], missing_tables=['prompt_versions'], **kwargs)
 prow=[dict(r) for r in conn.execute(f"SELECT {_expr(p,'id')}, {_expr(p,'name','prompt_name',alias='prompt_name')}, {_expr(p,'version','prompt_version',alias='version')}, {_expr(p,'approved_at','created_at',alias='approved_at')} FROM prompt_versions")]
 usage=[]
 for table in ('model_usage','pipeline_runs'):
  c=s.get(table)
  if c: usage += [dict(r) for r in conn.execute(f"SELECT {_expr(c,'prompt_name','name',alias='prompt_name')}, {_expr(c,'version','prompt_version','prompt_version_id',alias='version')}, {_expr(c,'created_at','started_at',alias='created_at')} FROM {table}")]
 return build_prompt_version_deployment_lag_report(prow, usage, **kwargs)
def format_prompt_version_deployment_lag_json(report): return _json(report)
def format_prompt_version_deployment_lag_text(report): return _text('Prompt Version Deployment Lag', report)
