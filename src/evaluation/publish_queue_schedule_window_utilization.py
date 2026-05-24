"""Publish Queue Schedule Window Utilization."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
from ._report_utils import clean,connection,dt,expr,json_dumps,loads_list,loads_obj,lower,now_iso,schema,to_float,to_int

ARTIFACT_TYPE='publish_queue_schedule_window_utilization'; DEFAULT_LIMIT=50
def _local(value,tz):
 parsed=dt(value)
 return parsed.astimezone(ZoneInfo(tz)) if parsed else None
def _dow(value): return value.strftime('%a').lower()[:3]
def _mins(text):
 parts=clean(text).split(':'); return int(parts[0])*60+int(parts[1]) if len(parts)>=2 else 0
def build_publish_queue_schedule_window_utilization_report(queue_items:list[dict[str,Any]],publish_windows:list[dict[str,Any]],*,timezone_name='UTC',underused_threshold:float=.5,overfilled_threshold:float=1.0,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
 if not (0<=underused_threshold<=1): raise ValueError('underused_threshold must be between 0 and 1')
 if overfilled_threshold<=0: raise ValueError('overfilled_threshold must be positive')
 if limit<=0: raise ValueError('limit must be positive')
 ZoneInfo(timezone_name)
 windows=[]
 for i,w in enumerate(publish_windows):
  cap=to_int(w.get('capacity') or w.get('max_items')) or 1; windows.append({'window_id':w.get('window_id') or w.get('id') or i+1,'platform':clean(w.get('platform'),'unknown').lower(),'day_of_week':lower(w.get('day_of_week') or w.get('weekday'))[:3],'start_time':clean(w.get('start_time')),'end_time':clean(w.get('end_time')),'capacity':cap})
 util=defaultdict(lambda:{'scheduled_count':0,'capacity':0,'items':[]}); outside=[]
 for i,item in enumerate(queue_items):
  scheduled=_local(item.get('scheduled_at') or item.get('published_at'),timezone_name); platform=clean(item.get('platform'),'unknown').lower(); matched=None
  if scheduled:
   m=scheduled.hour*60+scheduled.minute; day=_dow(scheduled)
   for w in windows:
    if w['platform']==platform and w['day_of_week']==day and _mins(w['start_time'])<=m<_mins(w['end_time']): matched=w; break
  if matched:
   key=(platform,scheduled.date().isoformat(),matched['window_id']); util[key]['scheduled_count']+=1; util[key]['capacity']=matched['capacity']; util[key]['window']=matched; util[key]['items'].append(item.get('queue_id') or item.get('id'))
  else:
   outside.append({'queue_id':item.get('queue_id') or item.get('id'),'platform':platform,'scheduled_at':scheduled.isoformat() if scheduled else clean(item.get('scheduled_at')) or None,'reason':'outside_window','_i':i})
 findings=[]
 for (p,day,wid),u in util.items():
  cap=max(u['capacity'],1); rate=round(u['scheduled_count']/cap,4); typ='overfilled' if rate>overfilled_threshold else 'underused' if rate<underused_threshold else None
  if typ: findings.append({'type':typ,'platform':p,'day':day,'window_id':wid,'scheduled_count':u['scheduled_count'],'capacity':cap,'utilization':rate})
 findings += [{k:v for k,v in x.items() if k!='_i'} for x in outside]
 findings.sort(key=lambda f:({'overfilled':0,'outside_window':1,'underused':2}.get(f.get('type') or f.get('reason'),9),f.get('platform',''),f.get('day',''),str(f.get('window_id',''))))
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'timezone':timezone_name,'underused_threshold':underused_threshold,'overfilled_threshold':overfilled_threshold,'limit':limit},'totals':{'queue_items':len(queue_items),'windows':len(windows),'findings':len(findings),'outside_window':len(outside),'shown_findings':len(findings[:limit])},'window_utilization':[{'platform':p,'day':d,'window_id':wid,'scheduled_count':u['scheduled_count'],'capacity':u['capacity'],'utilization':round(u['scheduled_count']/max(u['capacity'],1),4)} for (p,d,wid),u in sorted(util.items())],'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No publish queue schedule window utilization findings found.' if not findings else None}}
def build_publish_queue_schedule_window_utilization_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 if 'publish_queue' not in s: return build_publish_queue_schedule_window_utilization_report([],[],missing_tables=['publish_queue'],**kw)
 pc=s['publish_queue']; q=[dict(r) for r in conn.execute(f"SELECT {expr(pc,'id',out='id')}, {expr(pc,'platform',default='NULL',out='platform')}, {expr(pc,'scheduled_at','published_at',default='NULL',out='scheduled_at')}, {expr(pc,'status',default='NULL',out='status')} FROM publish_queue ORDER BY rowid")]
 wt=next((t for t in ('posting_windows','publish_windows') if t in s),None); wins=[]; missing=[] if wt else ['posting_windows|publish_windows']
 if wt:
  c=s[wt]; wins=[dict(r) for r in conn.execute(f"SELECT {expr(c,'id','window_id',default='NULL',out='window_id')}, {expr(c,'platform',default='NULL',out='platform')}, {expr(c,'day_of_week','weekday',default='NULL',out='day_of_week')}, {expr(c,'start_time',default='NULL',out='start_time')}, {expr(c,'end_time',default='NULL',out='end_time')}, {expr(c,'capacity','max_items',default='1',out='capacity')} FROM {wt} ORDER BY rowid")]
 return build_publish_queue_schedule_window_utilization_report(q,wins,missing_tables=missing,**kw)
def format_publish_queue_schedule_window_utilization_json(r): return json_dumps(r)
def format_publish_queue_schedule_window_utilization_text(r):
 lines=['Publish Queue Schedule Window Utilization',f"Generated: {r['generated_at']}",f"Timezone: {r['filters']['timezone']}",f"Totals: items={r['totals']['queue_items']} windows={r['totals']['windows']} findings={r['totals']['findings']}"]
 if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','type | platform | day | window_id | scheduled | capacity | utilization']
 for f in r['findings']: lines.append(f"{f.get('type') or f.get('reason')} | {f.get('platform')} | {f.get('day','-')} | {f.get('window_id','-')} | {f.get('scheduled_count','-')} | {f.get('capacity','-')} | {f.get('utilization','-')}")
 return '\n'.join(lines)
