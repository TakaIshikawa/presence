
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

from urllib.parse import urlparse
ARTIFACT='blog_draft_external_link_diversity'
def _domains(text):
    vals=[]
    for u in re.findall(r'https?://[^\s)\]"\']+', _clean(text)):
        vals.append(urlparse(u).netloc.lower().removeprefix('www.'))
    return vals
def build_blog_draft_external_link_diversity_report(rows, *, internal_domain=None, min_domains=2, max_domain_share=0.7, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
    if min_domains<=0 or not 0<max_domain_share<=1 or limit<=0: raise ValueError('invalid filter')
    gen=_utc(now or datetime.now(timezone.utc)); internal={d.lower().removeprefix('www.') for d in (internal_domain or [])}; findings=[]
    for r in rows:
        ds=[d for d in _domains(' '.join(_clean(r.get(k)) for k in ('body','content','metadata','links'))) if d not in internal]
        counts={d:ds.count(d) for d in sorted(set(ds))}; total=len(ds); unique=len(counts); reason=None
        top_share=max(counts.values())/total if total else 0
        if not total: reason='no_external_links'
        elif unique<min_domains: reason='low_domain_diversity'
        elif top_share>max_domain_share: reason='dominant_domain'
        if reason: findings.append({'draft_id':r.get('id') or r.get('draft_id'),'reason':reason,'external_link_count':total,'unique_domain_count':unique,'domains':counts,'dominant_domain_share':round(top_share,4)})
    findings.sort(key=lambda f:(f['reason'], _clean(f.get('draft_id')))); shown=findings[:limit]
    return {'artifact_type':ARTIFACT,'generated_at':gen.isoformat(),'filters':{'internal_domain':sorted(internal),'min_domains':min_domains,'max_domain_share':max_domain_share,'limit':limit},'totals':{'drafts':len(rows),'findings':len(findings),'shown_findings':len(shown)},'findings':shown,'missing_tables':missing_tables or [],'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No blog draft external link diversity issues found.' if not findings else None}}
def build_blog_draft_external_link_diversity_report_from_db(db, **kw):
    c=_conn(db); s=_schema(c); table='blog_drafts' if 'blog_drafts' in s else 'generated_content' if 'generated_content' in s else None
    if not table: return build_blog_draft_external_link_diversity_report([], missing_tables=['blog_drafts|generated_content'], **kw)
    cols=s[table]; rows=[dict(r) for r in c.execute(f"SELECT {_col(cols,'id','draft_id',fallback='rowid')} AS id,{_col(cols,'body','content','markdown',fallback='NULL')} AS body,{_col(cols,'metadata','links',fallback='NULL')} AS metadata FROM {table}")]
    return build_blog_draft_external_link_diversity_report(rows, **kw)
def format_blog_draft_external_link_diversity_json(report): return _json(report)
def format_blog_draft_external_link_diversity_text(report): return _text('Blog Draft External Link Diversity', report)
