"""Measure publish queue utilization against configured schedule windows."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='publish_queue_schedule_window_utilization'; DEFAULT_LIMIT=50; DEFAULT_UNDERUSED=0.5; DEFAULT_OVERFILLED=1.0
def _weekday(ts): return (dt(ts).strftime('%a').lower() if dt(ts) else 'unknown')
def _minute(text):
    parts=clean(text,'00:00').split(':'); return to_int(parts[0])*60+to_int(parts[1])
def _in_window(ts,w):
    d=dt(ts); 
    if not d: return False
    day=lower(w.get('day_of_week') or w.get('weekday'))
    if day and day!='all' and day[:3]!=d.strftime('%a').lower()[:3]: return False
    m=d.hour*60+d.minute; return _minute(w.get('start_time')) <= m < _minute(w.get('end_time') or '23:59')
def build_publish_queue_schedule_window_utilization_report(queue_rows:list[dict[str,Any]],window_rows:list[dict[str,Any]]|None=None,*,timezone:str='UTC',underused_threshold:float=DEFAULT_UNDERUSED,overfilled_threshold:float=DEFAULT_OVERFILLED,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    bounded_share('underused_threshold',underused_threshold); positive('overfilled_threshold',overfilled_threshold); positive('limit',limit); windows=window_rows or []; buckets=defaultdict(lambda:{'scheduled':0,'capacity':0}); findings=[]
    for w in windows:
        key=(clean(w.get('platform'),'unknown'),clean(w.get('day_of_week') or w.get('weekday'),'all'),clean(w.get('start_time'),'00:00')+'-'+clean(w.get('end_time'),'23:59')); buckets[key]['capacity']+=to_int(w.get('capacity') or w.get('max_items') or w.get('slot_count'),1)
    for q in queue_rows:
        ts=q.get('scheduled_at') or q.get('published_at'); p=clean(q.get('platform'),'unknown'); matches=[w for w in windows if clean(w.get('platform'),'unknown') in {p,'all'} and _in_window(ts,w)]
        if not matches:
            findings.append({'type':'outside_window','queue_id':q.get('queue_id') or q.get('id'),'platform':p,'scheduled_at':dt(ts).isoformat() if dt(ts) else None,'severity':50}); continue
        w=matches[0]; key=(clean(w.get('platform'),'unknown'),clean(w.get('day_of_week') or w.get('weekday'),'all'),clean(w.get('start_time'),'00:00')+'-'+clean(w.get('end_time'),'23:59')); buckets[key]['scheduled']+=1
    util=[]
    for (p,day,window),v in sorted(buckets.items()):
        rate=round(v['scheduled']/v['capacity'],4) if v['capacity'] else 0; util.append({'platform':p,'day':day,'window':window,'scheduled_count':v['scheduled'],'capacity':v['capacity'],'utilization':rate})
        if rate<underused_threshold: findings.append({'type':'underused_window','platform':p,'day':day,'window':window,'utilization':rate,'severity':round((underused_threshold-rate)*100,2)})
        if rate>overfilled_threshold: findings.append({'type':'overfilled_window','platform':p,'day':day,'window':window,'utilization':rate,'severity':round((rate-overfilled_threshold)*100,2)})
    findings.sort(key=lambda f:(-f['severity'],f['type'],f.get('platform','')))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'timezone':timezone,'underused_threshold':underused_threshold,'overfilled_threshold':overfilled_threshold,'limit':limit},'totals':{'queue_items':len(queue_rows),'windows':len(windows),'findings':len(findings)},'utilization':util,'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No publish queue schedule window utilization issues found.',schema_gap=bool(missing_tables or missing_columns))}
def build_publish_queue_schedule_window_utilization_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[] if 'publish_queue' in s else ['publish_queue']; mc={}; q=[]; w=[]
    if not mt: q=load_table(conn,'publish_queue',s['publish_queue'],{'queue_id':('id','queue_id'),'platform':('platform',),'scheduled_at':('scheduled_at','publish_at'),'published_at':('published_at',),'status':('status',)})
    for t in ('posting_windows','publish_windows'):
        if t in s: w+=load_table(conn,t,s[t],{'platform':('platform',),'day_of_week':('day_of_week','weekday'),'start_time':('start_time',),'end_time':('end_time',),'capacity':('capacity','max_items','slot_count')})
    return build_publish_queue_schedule_window_utilization_report(q,w,missing_tables=mt,missing_columns=mc,**kw)
def format_publish_queue_schedule_window_utilization_json(r): return json_dumps(r)
def format_publish_queue_schedule_window_utilization_text(r):
    lines=['Publish Queue Schedule Window Utilization',f"Generated: {r['generated_at']}",f"Totals: queue_items={r['totals']['queue_items']} windows={r['totals']['windows']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines += ['','type | platform | window | utilization']
    for f in r['findings']: lines.append(f"{f['type']} | {f.get('platform','-')} | {f.get('window','-')} | {f.get('utilization','-')}")
    return '\n'.join(lines)
