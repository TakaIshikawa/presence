"""Reply Draft Source License Exposure."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from typing import Any
from urllib.parse import urlparse
import re
from ._report_utils import clean,connection,dt,expr,json_dumps,lower,now_iso,schema,to_float,to_int

ARTIFACT_TYPE='reply_draft_source_license_exposure'; DEFAULT_LIMIT=50; BAD=('restricted','noncommercial','private','missing','unknown','nc')
def build_reply_draft_source_license_exposure_report(rows:list[dict[str,Any]],*,include_posted:bool=False,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
 if limit<=0: raise ValueError('limit must be positive')
 findings=[]
 for r in rows:
  status=lower(r.get('status'))
  if status=='posted' and not include_posted: continue
  lic=lower(r.get('license'),'missing') or 'missing'; reason=next((b for b in BAD if b in lic),None)
  if reason: findings.append({'reply_queue_id':r.get('reply_queue_id'),'status':status or None,'author':clean(r.get('author')) or None,'source_url':clean(r.get('source_url')) or None,'license':lic,'reason':'missing_license' if reason=='missing' else 'restricted_license'})
 findings.sort(key=lambda f:(f['reply_queue_id'] or 0,f['source_url'] or ''))
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'include_posted':include_posted,'limit':limit},'totals':{'rows':len(rows),'findings':len(findings),'shown_findings':len(findings[:limit])},'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No reply draft source license exposure found.' if not findings else None}}
def build_reply_draft_source_license_exposure_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 missing=[t for t in ('reply_queue','reply_knowledge_links','knowledge') if t not in s]
 if missing: return build_reply_draft_source_license_exposure_report([],missing_tables=missing,**kw)
 rq,lk,kn=s['reply_queue'],s['reply_knowledge_links'],s['knowledge']
 rows=[dict(r) for r in conn.execute(f"SELECT rq.{('id' if 'id' in rq else 'rowid')} AS reply_queue_id, {expr(rq,'status',default='NULL',alias='rq',out='status')}, {expr(rq,'author',default='NULL',alias='rq',out='author')}, {expr(kn,'source_url','url',default='NULL',alias='k',out='source_url')}, {expr(kn,'license',default='NULL',alias='k',out='license')} FROM reply_queue rq JOIN reply_knowledge_links l ON l.reply_queue_id=rq.id JOIN knowledge k ON k.id=l.knowledge_id ORDER BY rq.rowid")]
 return build_reply_draft_source_license_exposure_report(rows,**kw)
def format_reply_draft_source_license_exposure_json(r): return json_dumps(r)
def format_reply_draft_source_license_exposure_text(r):
 lines=['Reply Draft Source License Exposure',f"Generated: {r['generated_at']}",f"Totals: rows={r['totals']['rows']} findings={r['totals']['findings']}"]
 if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','reply_queue_id | status | author | license | reason | source_url']
 for f in r['findings']: lines.append(f"{f['reply_queue_id']} | {f['status']} | {f['author'] or '-'} | {f['license']} | {f['reason']} | {f['source_url'] or '-'}")
 return '\n'.join(lines)
