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

ARTIFACT='publication_attempt_request_body_growth'
def _bytes(v): return len(_clean(v).encode('utf-8'))
def build_publication_attempt_request_body_growth_report(rows, *, max_bytes=100000, growth_ratio=2.0, provider=None, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 if max_bytes<=0 or growth_ratio<=0 or limit<=0: raise ValueError('invalid filter')
 gen=_utc(now or datetime.now(timezone.utc)); rows=[r for r in rows if provider is None or _clean(r.get('provider'))==provider]; findings=[]; groups={}
 for r in rows:
  b=_bytes(r.get('request_payload') or r.get('request_body')); item={**r,'payload_bytes':b}; groups.setdefault((_clean(r.get('provider')),_clean(r.get('platform'))),[]).append(item)
  if b>max_bytes: findings.append({'attempt_id':r.get('id'),'provider':r.get('provider'),'platform':r.get('platform'),'reason':'max_payload_exceeded','payload_bytes':b})
 for (prov,plat), items in groups.items():
  vals=[i['payload_bytes'] for i in items]; mn=min(vals) if vals else 0; mx=max(vals) if vals else 0
  if mn and mx/mn>=growth_ratio and len(vals)>1: findings.append({'provider':prov,'platform':plat,'reason':'growth_ratio_exceeded','min_payload_bytes':mn,'max_payload_bytes':mx,'growth_ratio':round(mx/mn,4)})
 findings.sort(key=lambda f:(f['reason'], _clean(f.get('provider')), _clean(f.get('attempt_id')))); shown=findings[:limit]
 return {'artifact_type':ARTIFACT,'generated_at':gen.isoformat(),'filters':{'max_bytes':max_bytes,'growth_ratio':growth_ratio,'provider':provider,'limit':limit},'totals':{'attempts':len(rows),'findings':len(findings),'shown_findings':len(shown)},'findings':shown,'missing_tables':missing_tables or [],'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No publication attempt request body growth issues found.' if not findings else None}}
def build_publication_attempt_request_body_growth_report_from_db(db, **kw):
 c=_conn(db); s=_schema(c); cols=s.get('publication_attempts')
 if cols is None: return build_publication_attempt_request_body_growth_report([], missing_tables=['publication_attempts'], **kw)
 rows=[dict(r) for r in c.execute(f"SELECT {_col(cols,'id',fallback='rowid')} AS id,{_col(cols,'provider')} AS provider,{_col(cols,'platform')} AS platform,{_col(cols,'request_payload','request_body','body')} AS request_payload FROM publication_attempts")]
 return build_publication_attempt_request_body_growth_report(rows, **kw)
def format_publication_attempt_request_body_growth_json(r): return _json(r)
def format_publication_attempt_request_body_growth_text(r): return _text('Publication Attempt Request Body Growth', r)
