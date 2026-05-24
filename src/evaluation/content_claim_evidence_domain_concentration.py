"""Report claim evidence domain concentration."""
from __future__ import annotations
from collections import Counter,defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='content_claim_evidence_domain_concentration'; DEFAULT_LIMIT=50; DEFAULT_MIN_CLAIMS=2; DEFAULT_MAX_DOMAIN_SHARE=0.6
def build_content_claim_evidence_domain_concentration_report(rows:list[dict[str,Any]],*,min_claims:int=DEFAULT_MIN_CLAIMS,max_domain_share:float=DEFAULT_MAX_DOMAIN_SHARE,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive('min_claims',min_claims); bounded_share('max_domain_share',max_domain_share); positive('limit',limit); byc=defaultdict(list)
    for r in rows: byc[clean(r.get('content_id'),'unknown')].append(r)
    findings=[]; breakdown=[]
    for cid,items in byc.items():
        claims={clean(x.get('claim_id') or x.get('id'),str(i)) for i,x in enumerate(items)}
        if len(claims)<min_claims: continue
        ds=[domain(x.get('evidence_url') or x.get('url') or x.get('source_url')) for x in items if domain(x.get('evidence_url') or x.get('url') or x.get('source_url'))]
        cnt=Counter(ds); total=sum(cnt.values()); top,topn=(cnt.most_common(1)[0] if cnt else ('',0)); share=round(topn/total,4) if total else 0.0
        row={'content_id':cid,'claim_count':len(claims),'evidence_count':total,'domain_count':len(cnt),'top_domain':top or None,'top_domain_share':share,'domains':dict(sorted(cnt.items()))}; breakdown.append(row)
        if total==0 or share>max_domain_share or len(cnt)<2:
            reason='missing_evidence_domains' if total==0 else 'single_domain_concentration' if len(cnt)<2 else 'top_domain_overuse'
            findings.append({**row,'reason':reason,'severity':round((share-max_domain_share)*100 + (2-len(cnt))*20,2)})
    findings.sort(key=lambda f:(-f['severity'],f['content_id']))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'min_claims':min_claims,'max_domain_share':max_domain_share,'limit':limit},'totals':{'contents':len(byc),'evaluated':len(breakdown),'findings':len(findings)},'content_breakdown':sorted(breakdown,key=lambda r:r['content_id']),'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No content claim evidence domain concentration issues found.',schema_gap=bool(missing_tables or missing_columns))}
def build_content_claim_evidence_domain_concentration_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[] if 'content_claim_checks' in s else ['content_claim_checks']; mc={}; rows=[]
    if not mt:
        rows=load_table(conn,'content_claim_checks',s['content_claim_checks'],{'content_id':('content_id',),'claim_id':('claim_id','id'),'evidence_url':('evidence_url','source_url','url'),'claim_text':('claim_text','claim')})
    return build_content_claim_evidence_domain_concentration_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_content_claim_evidence_domain_concentration_json(r): return json_dumps(r)
def format_content_claim_evidence_domain_concentration_text(r):
    lines=['Content Claim Evidence Domain Concentration',f"Generated: {r['generated_at']}",f"Totals: contents={r['totals']['contents']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines += ['','content_id | reason | top_domain | share | claims']
    for f in r['findings']: lines.append(f"{f['content_id']} | {f['reason']} | {f['top_domain'] or '-'} | {f['top_domain_share']} | {f['claim_count']}")
    return '\n'.join(lines)
