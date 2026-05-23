"""Flag risky publication attempt payload sizes and traces."""
from ._focused_report_helpers import *
ARTIFACT_TYPE="publication_attempt_payload_size_risk"; DEFAULT_MAX_BYTES=65536; DEFAULT_TRACE_REPEAT_THRESHOLD=3; DEFAULT_WINDOW_DAYS=30
def build_publication_attempt_payload_size_risk_report(rows, *, max_bytes=DEFAULT_MAX_BYTES, trace_repeat_threshold=DEFAULT_TRACE_REPEAT_THRESHOLD, window_days=DEFAULT_WINDOW_DAYS, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 _positive('max_bytes',max_bytes); _positive('trace_repeat_threshold',trace_repeat_threshold); _positive('window_days',window_days); _positive('limit',limit); gen=_now(now); cutoff=gen-timedelta(days=window_days); findings=[]; scoped=[]
 for r in rows:
  dt=_dt(r.get('attempted_at') or r.get('created_at'));
  if dt and dt<cutoff: continue
  scoped.append(r); failed=_lower(r.get('status')) in {'failed','failure','error','errored','timeout'} or r.get('success') in (0,False,'0','false')
  texts=[('request_payload',r.get('request_payload')),('response_payload',r.get('response_payload')),('error_payload',r.get('error_payload'))]
  for name,val in texts:
   b=len(_clean(val).encode()) if val is not None else 0
   if b>max_bytes: findings.append(_f('oversized_payload',r,b,max_bytes,f'{name} exceeds threshold'))
   if val is not None and _clean(val).lower().count('traceback')>=trace_repeat_threshold: findings.append(_f('repeated_trace_payload',r,b,max_bytes,f'{name} repeats stack traces'))
  if failed and not any(_clean(v) for _,v in texts): findings.append(_f('missing_failure_payload',r,0,max_bytes,'failed attempt has no payload evidence'))
 return _finish(ARTIFACT_TYPE,gen,{'max_bytes':max_bytes,'trace_repeat_threshold':trace_repeat_threshold,'window_days':window_days,'limit':limit},len(scoped),findings,limit,missing_tables,missing_columns)
def _f(reason,r,b,t,detail): return {'reason':reason,'payload_bytes':b,'threshold_bytes':t,'attempt_id':r.get('attempt_id') or r.get('id'),'platform':r.get('platform'),'content_id':r.get('content_id'),'detail':detail}
def build_publication_attempt_payload_size_risk_report_from_db(db_or_conn, **kwargs):
 conn=_conn(db_or_conn); s=_schema(conn); c=s.get('publication_attempts')
 if c is None: return build_publication_attempt_payload_size_risk_report([], missing_tables=['publication_attempts'], **kwargs)
 sel=[_expr(c,'id',alias='attempt_id'),_expr(c,'content_id'),_expr(c,'platform'),_expr(c,'status'),_expr(c,'success'),_expr(c,'request_payload','request_metadata',alias='request_payload'),_expr(c,'response_payload','response_metadata',alias='response_payload'),_expr(c,'error_payload','error','error_message',alias='error_payload'),_expr(c,'attempted_at','created_at',alias='attempted_at')]
 return build_publication_attempt_payload_size_risk_report([dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM publication_attempts")], **kwargs)
def format_publication_attempt_payload_size_risk_json(report): return _json(report)
def format_publication_attempt_payload_size_risk_text(report): return _text('Publication Attempt Payload Size Risk', report)
