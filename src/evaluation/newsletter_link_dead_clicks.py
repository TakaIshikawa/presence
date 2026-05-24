"""Report clicks on dead or unknown newsletter links."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='newsletter_link_dead_clicks'; DEFAULT_MIN_CLICKS=1
def build_newsletter_link_dead_clicks_report(clicks:list[dict[str,Any]],inventory:list[dict[str,Any]]|None=None,*,since:str|None=None,min_clicks:int=DEFAULT_MIN_CLICKS,missing_tables=None,missing_columns=None,now=None):
    positive('min_clicks',min_clicks); since_dt=dt(since); inv={(clean(i.get('issue_id')), clean(i.get('url'))):i for i in (inventory or [])}; buckets={}
    for c in clicks:
        t=dt(c.get('clicked_at') or c.get('last_clicked_at'))
        if since_dt and t and t<since_dt: continue
        key=(clean(c.get('issue_id')), clean(c.get('url'))); b=buckets.setdefault(key,{'issue_id':key[0],'url':key[1],'click_count':0,'last_clicked_at':None})
        b['click_count']+=to_int(c.get('click_count'),1); b['last_clicked_at']=max([x for x in [b['last_clicked_at'], t.isoformat() if t else None] if x], default=None)
    findings=[]
    for key,b in buckets.items():
        item=inv.get(key); status=lower(item.get('status')) if item else 'unknown'
        if b['click_count']>=min_clicks and status in {'dead','error','missing','unknown','404','500'}:
            findings.append({**b,'status':status,'recommended_action':'replace_or_redirect' if status!='unknown' else 'add_to_inventory'})
    findings.sort(key=lambda f:(-f['click_count'], f.get('last_clicked_at') or '', f['url']))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'since':since,'min_clicks':min_clicks},'totals':{'click_rows':len(clicks),'inventory_rows':len(inventory or []),'findings':len(findings)},'findings':findings,'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No newsletter dead click issues found.',schema_gap=bool(missing_tables or missing_columns))}
def build_newsletter_link_dead_clicks_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; clicks=[]; inv=[]
    ctable=next((t for t in ('newsletter_link_clicks','newsletter_clicks') if t in s),None); itable='newsletter_link_inventory' if 'newsletter_link_inventory' in s else None
    if not ctable: mt.append('newsletter_link_clicks')
    else: clicks=load_table(conn,ctable,s[ctable],{'issue_id':('issue_id',),'url':('url',),'click_count':('click_count','clicks'),'clicked_at':('clicked_at','last_clicked_at','created_at')})
    if itable: inv=load_table(conn,itable,s[itable],{'issue_id':('issue_id',),'url':('url',),'status':('status','link_status')})
    return build_newsletter_link_dead_clicks_report(clicks,inv,missing_tables=mt,missing_columns={},**kw)
def format_newsletter_link_dead_clicks_json(r): return json_dumps(r)
def format_newsletter_link_dead_clicks_text(r):
    lines=['Newsletter Link Dead Clicks',f"Generated: {r['generated_at']}",f"Totals: clicks={r['totals']['click_rows']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines+=['','issue_id | url | status | clicks | action']
    for f in r['findings']: lines.append(f"{f['issue_id']} | {f['url']} | {f['status']} | {f['click_count']} | {f['recommended_action']}")
    return '\n'.join(lines)
