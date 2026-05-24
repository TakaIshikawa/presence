from __future__ import annotations
from datetime import datetime, timedelta, timezone
import json, re, sqlite3
from typing import Any
DEFAULT_LIMIT=100
def _conn(db):
 c=getattr(db,'conn',db)
 if not isinstance(c, sqlite3.Connection): raise TypeError('expected sqlite3.Connection or object with .conn')
 c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _clean(v): return '' if v is None else str(v).strip()
def _utc(v): return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
def _parse(v):
 s=_clean(v)
 if not s: return None
 try: return _utc(datetime.fromisoformat(s.replace('Z','+00:00')))
 except ValueError: return None
def _col(cols,*names,fallback='NULL'): return next((n for n in names if n in cols), fallback)
def _json(r): return json.dumps(r, indent=2, sort_keys=True)
def _text(title,r):
 lines=[title,'Totals: '+', '.join(f"{k}={v}" for k,v in sorted(r.get('totals',{}).items()))]
 if r.get('missing_tables'): lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if r.get('missing_columns'): lines.append('Missing columns: '+json.dumps(r['missing_columns'], sort_keys=True))
 if not r.get('findings'): lines.append(r.get('empty_state',{}).get('message') or 'No findings.')
 else:
  for f in r['findings']: lines.append('  - '+json.dumps(f, sort_keys=True))
 return '\n'.join(lines)

ARTIFACT='newsletter_segment_opt_out_drift'
def build_newsletter_segment_opt_out_drift_report(members, events, *, window_days=7, delta_threshold=.1, min_subscribers=1, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 if window_days<=0 or delta_threshold<0 or min_subscribers<=0 or limit<=0: raise ValueError('invalid filter')
 gen=_utc(now or datetime.now(timezone.utc)); cur_start=gen-timedelta(days=window_days); prev_start=gen-timedelta(days=window_days*2); segs=sorted({_clean(m.get('segment')) for m in members}); findings=[]
 for seg in segs:
  subs=len([m for m in members if _clean(m.get('segment'))==seg])
  if subs<min_subscribers: continue
  cur=prev=0
  for e in events:
   if _clean(e.get('segment'))!=seg: continue
   ts=_parse(e.get('occurred_at') or e.get('created_at'))
   if not ts: continue
   if cur_start<=ts<=gen: cur+=1
   elif prev_start<=ts<cur_start: prev+=1
  cr=cur/subs; pr=prev/subs; delta=cr-pr
  if delta>=delta_threshold: findings.append({'segment':seg,'previous_rate':round(pr,4),'current_rate':round(cr,4),'delta':round(delta,4),'subscriber_count':subs,'opt_out_count':cur})
 findings.sort(key=lambda f:(-f['delta'], f['segment'])); shown=findings[:limit]
 return {'artifact_type':ARTIFACT,'generated_at':gen.isoformat(),'filters':{'window_days':window_days,'delta_threshold':delta_threshold,'min_subscribers':min_subscribers,'limit':limit},'totals':{'segments':len(segs),'findings':len(findings),'shown_findings':len(shown)},'findings':shown,'missing_tables':missing_tables or [],'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No newsletter segment opt-out drift found.' if not findings else None}}
def build_newsletter_segment_opt_out_drift_report_from_db(db, **kw):
 c=_conn(db); s=_schema(c)
 if 'newsletter_segment_members' not in s: return build_newsletter_segment_opt_out_drift_report([],[], missing_tables=['newsletter_segment_members'], **kw)
 mc=s['newsletter_segment_members']; members=[dict(r) for r in c.execute(f"SELECT {_col(mc,'subscriber_id','email')} AS subscriber_id,{_col(mc,'segment','segment_id')} AS segment FROM newsletter_segment_members")]
 events=[]
 if 'newsletter_opt_out_events' in s:
  ec=s['newsletter_opt_out_events']; events=[dict(r) for r in c.execute(f"SELECT {_col(ec,'subscriber_id','email')} AS subscriber_id,{_col(ec,'segment','segment_id')} AS segment,{_col(ec,'occurred_at','created_at')} AS occurred_at FROM newsletter_opt_out_events")]
 return build_newsletter_segment_opt_out_drift_report(members, events, **kw)
def format_newsletter_segment_opt_out_drift_json(r): return _json(r)
def format_newsletter_segment_opt_out_drift_text(r): return _text('Newsletter Segment Opt Out Drift', r)
