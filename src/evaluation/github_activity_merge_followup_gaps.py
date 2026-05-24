
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

ARTIFACT='github_activity_merge_followup_gaps'
def build_github_activity_merge_followup_gaps_report(rows, followups=None, *, max_age_days=7, repository=None, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
    if max_age_days<=0 or limit<=0: raise ValueError('invalid filter')
    gen=_utc(now or datetime.now(timezone.utc)); followups=followups or []; keys={( _clean(f.get('repository')), _clean(f.get('pr_number')), _clean(f.get('source_activity_id'))) for f in followups}; findings=[]; scanned=0
    for r in rows:
        repo=_clean(r.get('repository')); pr=_clean(r.get('pr_number')); aid=_clean(r.get('activity_id') or r.get('id'))
        if repository and repo!=repository: continue
        scanned+=1; merged=_parse_time(r.get('merged_at') or r.get('created_at')) or gen; deadline=merged+timedelta(days=max_age_days)
        linked=any((repo,pr,aid)==k or (repo,pr,'')==k or ('','',aid)==k for k in keys)
        if not linked and deadline<=gen: findings.append({'activity_id':aid,'repository':repo,'pr_number':pr,'merged_at':merged.isoformat(),'deadline_at':deadline.isoformat(),'missing_followup_types':['content_idea','generated_content','newsletter_mention']})
    findings.sort(key=lambda f:(f['repository'], f['pr_number'], f['activity_id'])); shown=findings[:limit]
    return {'artifact_type':ARTIFACT,'generated_at':gen.isoformat(),'filters':{'max_age_days':max_age_days,'repository':repository,'limit':limit},'totals':{'merge_events':scanned,'findings':len(findings),'shown_findings':len(shown)},'findings':shown,'missing_tables':missing_tables or [],'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No GitHub activity merge follow-up gaps found.' if not findings else None}}
def build_github_activity_merge_followup_gaps_report_from_db(db, **kw):
    c=_conn(db); s=_schema(c); cols=s.get('github_activity')
    if cols is None: return build_github_activity_merge_followup_gaps_report([], missing_tables=['github_activity'], **kw)
    event_col=_col(cols,'event_type','type','action',fallback="''")
    rows=[dict(r) for r in c.execute(f"SELECT {_col(cols,'id','activity_id',fallback='rowid')} AS activity_id,{_col(cols,'repository','repo')} AS repository,{_col(cols,'pr_number','number')} AS pr_number,{_col(cols,'merged_at','created_at')} AS merged_at FROM github_activity WHERE lower(coalesce({event_col},'')) LIKE '%merge%'")]
    follows=[]
    for t in ('content_ideas','generated_content','newsletter_mentions'):
        if t in s:
            cs=s[t]; follows += [dict(r) for r in c.execute(f"SELECT {_col(cs,'repository','repo')} AS repository,{_col(cs,'pr_number')} AS pr_number,{_col(cs,'source_activity_id','github_activity_id')} AS source_activity_id FROM {t}")]
    return build_github_activity_merge_followup_gaps_report(rows, follows, **kw)
def format_github_activity_merge_followup_gaps_json(report): return _json(report)
def format_github_activity_merge_followup_gaps_text(report): return _text('GitHub Activity Merge Followup Gaps', report)
