"""Detect repeated pipeline stage visits without terminal outcomes."""
from ._focused_report_helpers import *
ARTIFACT_TYPE="pipeline_stage_retry_loops"; DEFAULT_MIN_REPEATS=3; DEFAULT_WINDOW_DAYS=30; TERMINAL={'success','succeeded','approved','published','rejected','failed','complete','completed'}
def build_pipeline_stage_retry_loops_report(rows, *, min_repeats=DEFAULT_MIN_REPEATS, window_days=DEFAULT_WINDOW_DAYS, stage=None, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 _positive('min_repeats',min_repeats); _positive('window_days',window_days); _positive('limit',limit); gen=_now(now); cutoff=gen-timedelta(days=window_days); groups=defaultdict(list)
 for r in rows:
  if stage and _clean(r.get('stage'))!=stage: continue
  dt=_dt(r.get('started_at') or r.get('created_at'))
  if dt and dt<cutoff: continue
  key=(r.get('content_id') or r.get('candidate_id'), _clean(r.get('stage'))); groups[key].append({**r,'dt':dt,'status':_lower(r.get('status') or r.get('outcome'))})
 findings=[]
 for (cid,st),rs in groups.items():
  terminal=any(r['status'] in TERMINAL for r in rs); base={'content_id':cid,'candidate_id':cid,'stage':st,'repeat_count':len(rs),'run_ids':[r.get('run_id') or r.get('id') for r in rs],'detail':'stage repeats without terminal outcome'}
  if len(rs)>=min_repeats: findings.append({**base,'reason':'repeated_stage_loop'})
  if len(rs)>=min_repeats and not terminal: findings.append({**base,'reason':'non_terminal_retry_loop'})
  if len(rs)>0 and not terminal: findings.append({**base,'reason':'missing_terminal_outcome','detail':'no terminal success/rejection state observed'})
 return _finish(ARTIFACT_TYPE,gen,{'min_repeats':min_repeats,'window_days':window_days,'stage':stage,'limit':limit},sum(len(v) for v in groups.values()),findings,limit,missing_tables,missing_columns)
def build_pipeline_stage_retry_loops_report_from_db(db_or_conn, **kwargs):
 conn=_conn(db_or_conn); s=_schema(conn); table='pipeline_events' if 'pipeline_events' in s else 'pipeline_runs' if 'pipeline_runs' in s else None
 if table is None: return build_pipeline_stage_retry_loops_report([], missing_tables=['pipeline_runs'], **kwargs)
 c=s[table]; sel=[_expr(c,'id','run_id',alias='run_id'),_expr(c,'content_id'),_expr(c,'candidate_id'),_expr(c,'stage','pipeline_stage',alias='stage'),_expr(c,'status','outcome',alias='status'),_expr(c,'started_at','created_at',alias='started_at'),_expr(c,'completed_at')]
 return build_pipeline_stage_retry_loops_report([dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM {table}")], **kwargs)
def format_pipeline_stage_retry_loops_json(report): return _json(report)
def format_pipeline_stage_retry_loops_text(report): return _text('Pipeline Stage Retry Loops', report)
