
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

ARTIFACT='content_claim_evidence_anchor_text_gaps'; DEFAULT_GENERIC=('click here','read more','source','link')
def _tokens(s): return set(re.findall(r'[a-z0-9]+', _clean(s).lower()))
def build_content_claim_evidence_anchor_text_gaps_report(rows, *, min_overlap=0.2, generic_anchor=None, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
    if not 0<=min_overlap<=1 or limit<=0: raise ValueError('invalid threshold')
    gen=_utc(now or datetime.now(timezone.utc)); generic={g.lower() for g in (generic_anchor or DEFAULT_GENERIC)}; counts={}; findings=[]
    for r in rows: counts[_clean(r.get('anchor_text')).lower()]=counts.get(_clean(r.get('anchor_text')).lower(),0)+1
    for r in rows:
        a=_clean(r.get('anchor_text')); ct=_clean(r.get('claim_text')); reasons=[]
        if not a: reasons.append('missing_anchor_text')
        if a.lower() in generic: reasons.append('generic_anchor_text')
        if a and counts.get(a.lower(),0)>1: reasons.append('repeated_anchor_text')
        overlap=len(_tokens(a)&_tokens(ct))/max(1,len(_tokens(a))) if a else 0
        if a and overlap<min_overlap: reasons.append('low_claim_overlap')
        for reason in reasons: findings.append({'evidence_id':r.get('id'),'claim_id':r.get('claim_id'),'reason':reason,'anchor_text':a,'claim_overlap':round(overlap,4)})
    findings.sort(key=lambda f:(f['reason'], _clean(f.get('evidence_id')))); shown=findings[:limit]
    return {'artifact_type':ARTIFACT,'generated_at':gen.isoformat(),'filters':{'min_overlap':min_overlap,'generic_anchor':sorted(generic),'limit':limit},'totals':{'rows':len(rows),'findings':len(findings),'shown_findings':len(shown)},'findings':shown,'missing_tables':missing_tables or [],'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No content claim evidence anchor text gaps found.' if not findings else None}}
def build_content_claim_evidence_anchor_text_gaps_report_from_db(db, **kw):
    c=_conn(db); s=_schema(c); cols=s.get('content_claim_evidence')
    if cols is None: return build_content_claim_evidence_anchor_text_gaps_report([], missing_tables=['content_claim_evidence'], **kw)
    claim_expr=_col(cols,'claim_text','text',fallback='NULL')
    if claim_expr=='NULL' and 'content_claims' in s and 'claim_id' in cols:
        rows=[dict(r) for r in c.execute(f"SELECT e.rowid AS id,e.claim_id,e.{_col(cols,'anchor_text')} AS anchor_text,c.{_col(s['content_claims'],'claim_text','text')} AS claim_text FROM content_claim_evidence e LEFT JOIN content_claims c ON c.id=e.claim_id")]
    else:
        rows=[dict(r) for r in c.execute(f"SELECT {_col(cols,'id',fallback='rowid')} AS id,{_col(cols,'claim_id')} AS claim_id,{_col(cols,'anchor_text')} AS anchor_text,{claim_expr} AS claim_text FROM content_claim_evidence")]
    return build_content_claim_evidence_anchor_text_gaps_report(rows, **kw)
def format_content_claim_evidence_anchor_text_gaps_json(report): return _json(report)
def format_content_claim_evidence_anchor_text_gaps_text(report): return _text('Content Claim Evidence Anchor Text Gaps', report)
