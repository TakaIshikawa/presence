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

ARTIFACT='content_variant_localization_readiness'
def build_content_variant_localization_readiness_report(rows, *, required_locale=None, platform=None, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 gen=_utc(now or datetime.now(timezone.utc)); req=required_locale or []; rows=[r for r in rows if platform is None or _clean(r.get('platform'))==platform]; findings=[]; seen={( _clean(r.get('content_id')), _clean(r.get('locale')), _clean(r.get('platform'))) for r in rows}
 for r in rows:
  loc=_clean(r.get('locale')); body=_clean(r.get('body') or r.get('content')); src=_clean(r.get('source_body')); cta=_clean(r.get('cta') or r.get('localized_cta'))
  if not loc: findings.append({'content_id':r.get('content_id'),'locale':loc,'platform':r.get('platform'),'reason':'missing_locale'})
  if src and body==src: findings.append({'content_id':r.get('content_id'),'locale':loc,'platform':r.get('platform'),'reason':'untranslated_body'})
  if loc and loc!='en' and not cta: findings.append({'content_id':r.get('content_id'),'locale':loc,'platform':r.get('platform'),'reason':'missing_localized_cta'})
 for cid in sorted({_clean(r.get('content_id')) for r in rows}):
  plats=sorted({_clean(r.get('platform')) for r in rows if _clean(r.get('content_id'))==cid}) or ['']
  for loc in req:
   for pl in plats:
    if (cid,loc,pl) not in seen: findings.append({'content_id':cid,'locale':loc,'platform':pl,'reason':'missing_platform_locale_variant'})
 findings.sort(key=lambda f:(_clean(f.get('content_id')), _clean(f.get('locale')), f['reason'])); shown=findings[:limit]
 return {'artifact_type':ARTIFACT,'generated_at':gen.isoformat(),'filters':{'required_locale':req,'platform':platform,'limit':limit},'totals':{'variants':len(rows),'findings':len(findings),'shown_findings':len(shown)},'findings':shown,'missing_tables':missing_tables or [],'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No content variant localization readiness issues found.' if not findings else None}}
def build_content_variant_localization_readiness_report_from_db(db, **kw):
 c=_conn(db); s=_schema(c); cols=s.get('content_variants')
 if cols is None: return build_content_variant_localization_readiness_report([], missing_tables=['content_variants'], **kw)
 rows=[dict(r) for r in c.execute(f"SELECT {_col(cols,'id',fallback='rowid')} AS id,{_col(cols,'content_id')} AS content_id,{_col(cols,'locale','language')} AS locale,{_col(cols,'platform')} AS platform,{_col(cols,'body','content')} AS body,{_col(cols,'source_body','source_content')} AS source_body,{_col(cols,'cta','localized_cta')} AS cta FROM content_variants")]
 return build_content_variant_localization_readiness_report(rows, **kw)
def format_content_variant_localization_readiness_json(r): return _json(r)
def format_content_variant_localization_readiness_text(r): return _text('Content Variant Localization Readiness', r)
