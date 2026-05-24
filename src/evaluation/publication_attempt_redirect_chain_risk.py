"""Flag publication attempt redirect chain risks."""
from __future__ import annotations
from collections import Counter,defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='publication_attempt_redirect_chain_risk'; DEFAULT_LIMIT=50; DEFAULT_MAX_HOPS=3; SHORT={'bit.ly','t.co','tinyurl.com','goo.gl','ow.ly'}
def build_publication_attempt_redirect_chain_risk_report(rows:list[dict[str,Any]],*,max_hops:int=DEFAULT_MAX_HOPS,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    non_negative('max_hops',max_hops); positive('limit',limit); findings=[]; platform=defaultdict(lambda:{'attempts':0,'risky':0}); maxh=defaultdict(int)
    for i,r in enumerate(rows):
        status=lower(r.get('status'))
        if status not in {'success','succeeded','published','sent','ok'}: continue
        p=clean(r.get('platform'),'unknown'); platform[p]['attempts']+=1; hops=to_int(r.get('redirect_hops')) ; maxh[p]=max(maxh[p],hops); url=clean(r.get('url')); final=clean(r.get('final_url')); reasons=[]
        if hops>max_hops: reasons.append('too_many_hops')
        if not final: reasons.append('missing_final_url')
        if url.startswith('https://') and final.startswith('http://'): reasons.append('http_downgrade')
        if final and domain(url) and domain(final) and domain(url)!=domain(final): reasons.append('final_domain_mismatch')
        if domain(url) in SHORT or domain(final) in SHORT: reasons.append('shortened_link_chain')
        if reasons:
            platform[p]['risky']+=1; findings.append({'attempt_id':r.get('attempt_id') or r.get('id') or i+1,'platform':p,'content_id':r.get('content_id'),'url':url or None,'final_url':final or None,'redirect_hops':hops,'risk_reasons':sorted(reasons),'checked_at':(dt(r.get('checked_at')) or dt(r.get('created_at')) or None).isoformat() if (dt(r.get('checked_at')) or dt(r.get('created_at'))) else None,'severity':len(reasons)*10+hops})
    findings.sort(key=lambda f:(-f['severity'],f['platform'],str(f['attempt_id'])))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'max_hops':max_hops,'limit':limit},'totals':{'attempts':sum(v['attempts'] for v in platform.values()),'findings':len(findings)},'platform_totals':dict(sorted(platform.items())),'max_hop_summaries':[{'platform':p,'max_redirect_hops':h} for p,h in sorted(maxh.items())],'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No publication attempt redirect chain risks found.',schema_gap=bool(missing_tables or missing_columns))}
def build_publication_attempt_redirect_chain_risk_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[] if 'publication_attempts' in s else ['publication_attempts']; mc={}; rows=[]
    if not mt:
        cols=s['publication_attempts']; rows=load_table(conn,'publication_attempts',cols,{'attempt_id':('id','attempt_id'),'platform':('platform',),'content_id':('content_id',),'url':('url','published_url'),'final_url':('final_url',),'redirect_hops':('redirect_hops',),'status':('status',),'checked_at':('checked_at','created_at')})
    for t in ('url_check_results','link_check_results'):
        pass
    return build_publication_attempt_redirect_chain_risk_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_publication_attempt_redirect_chain_risk_json(r): return json_dumps(r)
def format_publication_attempt_redirect_chain_risk_text(r):
    lines=['Publication Attempt Redirect Chain Risk',f"Generated: {r['generated_at']}",f"Totals: attempts={r['totals']['attempts']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines += ['','attempt_id | platform | hops | reasons | final_url']
    for f in r['findings']: lines.append(f"{f['attempt_id']} | {f['platform']} | {f['redirect_hops']} | {', '.join(f['risk_reasons'])} | {f['final_url'] or '-'}")
    return '\n'.join(lines)
