"""Content Claim Evidence Domain Concentration."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from statistics import median
from typing import Any
from urllib.parse import urlparse
from ._report_utils import clean,connection,dt,expr,json_dumps,loads_list,loads_obj,lower,now_iso,schema,to_float,to_int

ARTIFACT_TYPE='content_claim_evidence_domain_concentration'; DEFAULT_LIMIT=50
def _domain(u): return (urlparse(clean(u)).netloc or '').lower().removeprefix('www.')
def build_content_claim_evidence_domain_concentration_report(rows:list[dict[str,Any]],*,min_claims:int=2,max_domain_share:float=.7,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
 if min_claims<=0 or limit<=0: raise ValueError('positive values required')
 if not 0<max_domain_share<=1: raise ValueError('max_domain_share must be between 0 and 1')
 grouped=defaultdict(list)
 for r in rows: grouped[clean(r.get('content_id'),'unknown')].append(r)
 findings=[]; breakdown=[]
 for cid,items in grouped.items():
  if len(items)<min_claims: continue
  domains=[_domain(x.get('evidence_url') or x.get('url')) for x in items if _domain(x.get('evidence_url') or x.get('url'))]
  counts=Counter(domains); top,count=counts.most_common(1)[0] if counts else ('',0); share=round(count/max(len(domains),1),4)
  row={'content_id':cid,'claim_count':len(items),'evidence_domains':len(counts),'top_domain':top or None,'top_domain_share':share}
  breakdown.append(row)
  if counts and share>max_domain_share: findings.append({**row,'reason':'single_domain_concentration'})
 findings.sort(key=lambda f:(-f['top_domain_share'],f['content_id']))
 return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'min_claims':min_claims,'max_domain_share':max_domain_share,'limit':limit},'totals':{'contents':len(grouped),'findings':len(findings),'shown_findings':len(findings[:limit])},'content_breakdown':sorted(breakdown,key=lambda r:r['content_id']),'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':missing_columns or {},'empty_state':{'is_empty':not findings,'message':'No content claim evidence domain concentration findings found.' if not findings else None}}
def build_content_claim_evidence_domain_concentration_report_from_db(db_or_conn:Any,**kw):
 conn=connection(db_or_conn); s=schema(conn)
 if 'content_claim_checks' not in s: return build_content_claim_evidence_domain_concentration_report([],missing_tables=['content_claim_checks'],**kw)
 c=s['content_claim_checks']; rows=[dict(r) for r in conn.execute(f"SELECT {expr(c,'content_id',default='NULL',out='content_id')}, {expr(c,'claim_id','id',default='NULL',out='claim_id')}, {expr(c,'evidence_url','url','source_url',default='NULL',out='evidence_url')} FROM content_claim_checks ORDER BY rowid")]
 return build_content_claim_evidence_domain_concentration_report(rows,**kw)
def format_content_claim_evidence_domain_concentration_json(r): return json_dumps(r)
def format_content_claim_evidence_domain_concentration_text(r):
 lines=['Content Claim Evidence Domain Concentration',f"Generated: {r['generated_at']}",f"Totals: contents={r['totals']['contents']} findings={r['totals']['findings']}"]
 if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
 if not r['findings']: lines.append(r['empty_state']['message'] or 'No findings.'); return '\n'.join(lines)
 lines+=['','content_id | claim_count | top_domain | top_domain_share | reason']
 for f in r['findings']: lines.append(f"{f['content_id']} | {f['claim_count']} | {f['top_domain']} | {f['top_domain_share']} | {f['reason']}")
 return '\n'.join(lines)
