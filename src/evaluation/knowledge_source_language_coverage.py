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

ARTIFACT='knowledge_source_language_coverage'
def build_knowledge_source_language_coverage_report(rows, *, supported_language=None, max_language_share=.8, campaign_id=None, limit=DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
 if not 0<max_language_share<=1 or limit<=0: raise ValueError('invalid filter')
 gen=_utc(now or datetime.now(timezone.utc)); supported={x.lower() for x in (supported_language or ['en'])}; rows=[r for r in rows if campaign_id is None or _clean(r.get('campaign_id'))==_clean(campaign_id)]
 counts={}; findings=[]; by_campaign={}
 for r in rows:
  lang=_clean(r.get('language') or r.get('locale')).lower(); counts[lang or 'missing']=counts.get(lang or 'missing',0)+1; by_campaign.setdefault(_clean(r.get('campaign_id')),[]).append(lang or 'missing')
  if not lang: findings.append({'source_id':r.get('id'),'campaign_id':r.get('campaign_id'),'reason':'missing_language'})
  elif lang not in supported: findings.append({'source_id':r.get('id'),'campaign_id':r.get('campaign_id'),'language':lang,'reason':'unsupported_language'})
 for camp, langs in by_campaign.items():
  if camp and langs:
   top=max(langs,key=langs.count); share=langs.count(top)/len(langs)
   if len(langs)>1 and share>max_language_share: findings.append({'campaign_id':camp,'reason':'campaign_language_skew','language':top,'share':round(share,4)})
 findings.sort(key=lambda f:(f['reason'], _clean(f.get('campaign_id')), _clean(f.get('source_id')))); shown=findings[:limit]
 return {'artifact_type':ARTIFACT,'generated_at':gen.isoformat(),'filters':{'supported_language':sorted(supported),'max_language_share':max_language_share,'campaign_id':campaign_id,'limit':limit},'totals':{'sources':len(rows),'by_language':dict(sorted(counts.items())),'findings':len(findings),'shown_findings':len(shown)},'by_language':dict(sorted(counts.items())),'findings':shown,'missing_tables':missing_tables or [],'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No knowledge source language coverage issues found.' if not findings else None}}
def build_knowledge_source_language_coverage_report_from_db(db, **kw):
 c=_conn(db); s=_schema(c); cols=s.get('knowledge_sources')
 if cols is None: return build_knowledge_source_language_coverage_report([], missing_tables=['knowledge_sources'], **kw)
 rows=[dict(r) for r in c.execute(f"SELECT {_col(cols,'id',fallback='rowid')} AS id,{_col(cols,'language','locale')} AS language,{_col(cols,'campaign_id')} AS campaign_id FROM knowledge_sources")]
 return build_knowledge_source_language_coverage_report(rows, **kw)
def format_knowledge_source_language_coverage_json(r): return _json(r)
def format_knowledge_source_language_coverage_text(r): return _text('Knowledge Source Language Coverage', r)
