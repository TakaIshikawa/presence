"""Publication Attempt Redirect Chain Risk."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
from ._report_utils import clean,connection,dt,expr,json_dumps,loads_list,loads_obj,lower,now_iso,schema,to_float,to_int

ARTIFACT_TYPE='publication_attempt_redirect_chain_risk'; DEFAULT_LIMIT=50; DEFAULT_MAX_HOPS=3; SHORT={'bit.ly','t.co','tinyurl.com','goo.gl','buff.ly'}
def _domain(u): return (urlparse(clean(u)).netloc or '').lower().removeprefix('www.')
def build_publication_attempt_redirect_chain_risk_report(rows:list[dict[str,Any]],*,max_hops:int=DEFAULT_MAX_HOPS,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
 if max_hops<0: raise ValueError('max_hops must be non-negative')
 if limit<=0: raise ValueError('limit must be positive')
 findings=[]; plat=defaultdict(lambda:{'attempts':0,'findings':0,'max_redirect_hops':0})
 for i,r in enumerate(rows):
  if lower(r.get('status')) not in {'success','published','succeeded'}: continue
  p=clean(r.get('platform'),'unknown').lower(); hops=to_int(r.get('redirect_hops')) or 0; reasons=[]; plat[p]['attempts']+=1; plat[p]['max_redirect_hops']=max(plat[p]['max_redirect_hops'],hops)
  if hops>max_hops: reasons.append('too_many_hops')
  if not clean(r.get('final_url')): reasons.append('missing_final_url')
  if _domain(r.get('url')) and _domain(r.get('final_url')) and _domain(r.get('url'))!=_domain(r.get('final_url')): reasons.append('final_domain_mismatch')
  if clean(r.get('url')).startswith('https://') and clean(r.get('final_url')).startswith('http://'): reasons.append('http_downgrade')
  if _domain(r.get('url')) in SHORT or _domain(r.get('final_url')) in SHORT: reasons.append('shortened_link_chain')
  if reasons:
   plat[p]['findings']+=1; findings.append({'attempt_id':r.get('attempt_id') or r.get('id'),'platform':p,'content_id':r.get('content_id'),'url':clean(r.get('url')) or None,'final_url':clean(r.get('final_url')) or None,'redirect_hops':hops,'checked_at':clean(r.get('checked_at')) or None,'risk_reasons':reasons,'severity':len(reasons)+hops,'_i':i})
 findings.sort(key=lambda f:(-f['severity'],f['platform'],f['_i']))
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'max_hops':max_hops,'limit':limit},'totals':{'attempts':sum(x['attempts'] for x in plat.values()),'findings':len(findings),'shown_findings':len(findings[:limit])},'platform_totals':[{'platform':k,**v} for k,v in sorted(plat.items())],'max_hop_summary':dict(sorted((k,v['max_redirect_hops']) for k,v in plat.items())),'findings':[{k:v for k,v in f.items() if k!='_i'} for f in findings[:limit]],'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No publication redirect chain risks found.' if not findings else None}}
def build_publication_attempt_redirect_chain_risk_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 if 'publication_attempts' not in s: return build_publication_attempt_redirect_chain_risk_report([],missing_tables=['publication_attempts'],**kw)
 c=s['publication_attempts']; rows=[dict(r) for r in conn.execute(f"SELECT {expr(c,'id',out='attempt_id')}, {expr(c,'platform',default='NULL',out='platform')}, {expr(c,'content_id',default='NULL',out='content_id')}, {expr(c,'url',default='NULL',out='url')}, {expr(c,'final_url',default='NULL',out='final_url')}, {expr(c,'redirect_hops',default='0',out='redirect_hops')}, {expr(c,'status',default='NULL',out='status')}, {expr(c,'checked_at','created_at',default='NULL',out='checked_at')} FROM publication_attempts ORDER BY rowid")]
 return build_publication_attempt_redirect_chain_risk_report(rows,missing_tables=[] if ('url_check_results' in s or 'link_check_results' in s) else ['url_check_results|link_check_results'],**kw)
def format_publication_attempt_redirect_chain_risk_json(r): return json_dumps(r)
def format_publication_attempt_redirect_chain_risk_text(r):
 lines=['Publication Attempt Redirect Chain Risk',f"Generated: {r['generated_at']}",f"Totals: attempts={r['totals']['attempts']} findings={r['totals']['findings']}"]
 if r['missing_tables']: lines.append('Missing optional tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','attempt_id | platform | redirect_hops | reasons']
 for f in r['findings']: lines.append(f"{f['attempt_id']} | {f['platform']} | {f['redirect_hops']} | {', '.join(f['risk_reasons'])}")
 return '\n'.join(lines)
