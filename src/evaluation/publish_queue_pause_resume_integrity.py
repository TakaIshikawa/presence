
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import json, re, sqlite3
from typing import Any

DEFAULT_LIMIT=100

def _conn(db):
    c=getattr(db,'conn',db)
    if not isinstance(c, sqlite3.Connection): raise TypeError('expected sqlite3.Connection or object with .conn')
    c.row_factory=sqlite3.Row; return c

def _schema(c):
    return {str(r[0]): {str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}

def _clean(v): return '' if v is None else str(v).strip()
def _utc(v): return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
def _parse_time(v):
    s=_clean(v)
    if not s: return None
    try: return _utc(datetime.fromisoformat(s.replace('Z','+00:00')))
    except ValueError: return None

def _col(cols,*names,fallback='NULL'):
    return next((n for n in names if n in cols), fallback)
def _missing_report(artifact, filters, table):
    return {'artifact_type':artifact,'generated_at':_utc(datetime.now(timezone.utc)).isoformat(),'filters':filters,'totals':{},'findings':[],'missing_tables':[table],'missing_columns':{},'empty_state':{'is_empty':True,'message':'Required table is missing.'}}
def _json(report): return json.dumps(report, indent=2, sort_keys=True)
def _text(title, report):
    lines=[title, 'Totals: '+', '.join(f"{k}={v}" for k,v in sorted(report.get('totals',{}).items()))]
    if report.get('missing_tables'): lines.append('Missing tables: '+', '.join(report['missing_tables']))
    if report.get('missing_columns'): lines.append('Missing columns: '+json.dumps(report['missing_columns'], sort_keys=True))
    if not report.get('findings'): lines.append(report.get('empty_state',{}).get('message') or 'No findings.')
    else:
        for f in report['findings']: lines.append('  - '+json.dumps(f, sort_keys=True))
    return '\n'.join(lines)

ARTIFACT='publish_queue_pause_resume_integrity'
def build_publish_queue_pause_resume_integrity_report(rows, *, max_pause_hours=24, actor=None, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
    if max_pause_hours<=0 or limit<=0: raise ValueError('max_pause_hours and limit must be positive')
    gen=_utc(now or datetime.now(timezone.utc)); findings=[]; state={}; scanned=0
    for r in sorted(rows, key=lambda x: (_clean(x.get('occurred_at')), _clean(x.get('id')))):
        if actor and _clean(r.get('actor'))!=actor: continue
        scanned+=1; q=_clean(r.get('queue_item_id')); kind=_clean(r.get('event_type') or r.get('type')).lower(); ts=_parse_time(r.get('occurred_at') or r.get('created_at')) or gen
        if kind=='pause':
            if q in state: findings.append({'queue_item_id':q,'reason':'overlapping_pause','event_id':r.get('id'),'paused_at':state[q]['ts'].isoformat(),'overlap_at':ts.isoformat()})
            state[q]={'ts':ts,'id':r.get('id'),'actor':r.get('actor')}
        elif kind=='resume':
            if q not in state: findings.append({'queue_item_id':q,'reason':'orphan_resume','event_id':r.get('id'),'resumed_at':ts.isoformat()})
            else: state.pop(q)
    for q,s in state.items():
        hrs=(gen-s['ts']).total_seconds()/3600
        if hrs>max_pause_hours: findings.append({'queue_item_id':q,'reason':'pause_duration_exceeded','event_id':s['id'],'paused_at':s['ts'].isoformat(),'pause_hours':round(hrs,2)})
    findings.sort(key=lambda f:(f['reason'], _clean(f.get('queue_item_id')))); shown=findings[:limit]
    return {'artifact_type':ARTIFACT,'generated_at':gen.isoformat(),'filters':{'max_pause_hours':max_pause_hours,'actor':actor,'limit':limit},'totals':{'events':scanned,'findings':len(findings),'shown_findings':len(shown)},'findings':shown,'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No publish queue pause/resume integrity issues found.' if not findings else None}}
def build_publish_queue_pause_resume_integrity_report_from_db(db, **kw):
    c=_conn(db); s=_schema(c); cols=s.get('publish_queue_events')
    if cols is None: return build_publish_queue_pause_resume_integrity_report([], missing_tables=['publish_queue_events'], **kw)
    sel=f"SELECT {_col(cols,'id','event_id',fallback='rowid')} AS id,{_col(cols,'queue_item_id','item_id')} AS queue_item_id,{_col(cols,'event_type','type','action')} AS event_type,{_col(cols,'actor')} AS actor,{_col(cols,'occurred_at','created_at')} AS occurred_at FROM publish_queue_events ORDER BY occurred_at ASC, id ASC"
    return build_publish_queue_pause_resume_integrity_report([dict(r) for r in c.execute(sel)], **kw)
def format_publish_queue_pause_resume_integrity_json(report): return _json(report)
def format_publish_queue_pause_resume_integrity_text(report): return _text('Publish Queue Pause Resume Integrity', report)
