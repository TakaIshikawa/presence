"""Detect lower-priority publish queue items moving before higher-priority ready items."""
from ._focused_report_helpers import *
ARTIFACT_TYPE="publish_queue_priority_inversion"; DEFAULT_WINDOW_HOURS=168; DEFAULT_MIN_PRIORITY_GAP=1
def build_publish_queue_priority_inversion_report(rows, *, window_hours=DEFAULT_WINDOW_HOURS, min_priority_gap=DEFAULT_MIN_PRIORITY_GAP, platform=None, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 _positive('window_hours',window_hours); _nonneg('min_priority_gap',min_priority_gap); _positive('limit',limit); gen=_now(now); cutoff=gen-timedelta(hours=window_hours); norm=[]
 for r in rows:
  if platform and _lower(r.get('platform'))!=platform.lower(): continue
  dt=_dt(r.get('scheduled_at') or r.get('completed_at') or r.get('created_at'))
  if dt and dt<cutoff: continue
  norm.append({**r,'priority':_int(r.get('priority')) or 0,'when':dt})
 findings=[]
 for hi in norm:
  for lo in norm:
   if hi is lo or hi.get('platform')!=lo.get('platform') or hi.get('campaign_id')!=lo.get('campaign_id'): continue
   if hi['priority']-lo['priority']<min_priority_gap: continue
   if hi['when'] and lo['when'] and lo['when']<hi['when']:
    findings.append({'reason':'priority_inversion','blocked_queue_id':hi.get('queue_id') or hi.get('id'),'earlier_queue_id':lo.get('queue_id') or lo.get('id'),'platform':hi.get('platform'),'blocked_priority':hi['priority'],'earlier_priority':lo['priority'],'blocked_scheduled_at':hi['when'].isoformat(),'earlier_scheduled_at':lo['when'].isoformat(),'campaign_id':hi.get('campaign_id'),'inversion_minutes':round((hi['when']-lo['when']).total_seconds()/60,2),'detail':'lower-priority item scheduled/completed first'})
 return _finish(ARTIFACT_TYPE,gen,{'window_hours':window_hours,'min_priority_gap':min_priority_gap,'platform':platform,'limit':limit},len(norm),findings,limit,missing_tables,missing_columns)
def build_publish_queue_priority_inversion_report_from_db(db_or_conn, **kwargs):
 conn=_conn(db_or_conn); s=_schema(conn); c=s.get('publish_queue')
 if c is None: return build_publish_queue_priority_inversion_report([], missing_tables=['publish_queue'], **kwargs)
 sel=[_expr(c,'id',alias='queue_id'),_expr(c,'content_id'),_expr(c,'platform'),_expr(c,'priority'),_expr(c,'campaign_id'),_expr(c,'scheduled_at','completed_at','created_at',alias='scheduled_at'),_expr(c,'status')]
 return build_publish_queue_priority_inversion_report([dict(r) for r in conn.execute(f"SELECT {', '.join(sel)} FROM publish_queue")], **kwargs)
def format_publish_queue_priority_inversion_json(report): return _json(report)
def format_publish_queue_priority_inversion_text(report): return _text('Publish Queue Priority Inversion', report)
