"""Flag reply drafts with risky source licenses."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='reply_draft_source_license_exposure'; DEFAULT_LIMIT=50; RISK=['restricted','noncommercial','non-commercial','private','missing','unknown','proprietary']
def _reason(lic):
    l=lower(lic,'missing')
    if not l or l=='missing': return 'missing_license'
    if 'noncommercial' in l or 'non-commercial' in l or 'nc'==l: return 'noncommercial_license'
    if 'private' in l: return 'private_source'
    if 'restrict' in l or 'proprietary' in l: return 'restricted_license'
    return None
def build_reply_draft_source_license_exposure_report(rows:list[dict[str,Any]],*,include_posted:bool=False,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive('limit',limit); findings=[]
    for i,r in enumerate(rows):
        status=lower(r.get('status'),'draft')
        if status in {'posted','sent','published'} and not include_posted: continue
        reason=_reason(r.get('license'))
        if reason: findings.append({'reply_queue_id':r.get('reply_queue_id') or r.get('id') or i+1,'status':status,'author':clean(r.get('author') or r.get('author_id'),'unknown'),'source_url':clean(r.get('source_url') or r.get('url')) or None,'license':clean(r.get('license'),'missing'),'reason':reason,'severity':{'restricted_license':60,'private_source':55,'noncommercial_license':45,'missing_license':30}.get(reason,20)})
    findings.sort(key=lambda f:(-f['severity'],str(f['reply_queue_id']),f['source_url'] or ''))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'include_posted':include_posted,'limit':limit},'totals':{'rows':len(rows),'findings':len(findings)},'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No reply draft source license exposure found.',schema_gap=bool(missing_tables or missing_columns))}
def build_reply_draft_source_license_exposure_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[] if 'reply_queue' in s else ['reply_queue']; mc={}; rows=[]
    if 'reply_queue' in s and 'reply_knowledge_links' in s and 'knowledge' in s:
        q=s['reply_queue']; l=s['reply_knowledge_links']; k=s['knowledge']
        rows=[dict(r) for r in conn.execute("SELECT q.id AS reply_queue_id, q.status AS status, q.author AS author, k.url AS source_url, k.license AS license FROM reply_queue q JOIN reply_knowledge_links l ON l.reply_queue_id=q.id JOIN knowledge k ON k.id=l.knowledge_id ORDER BY q.id")]
    elif 'reply_queue' in s:
        rows=load_table(conn,'reply_queue',s['reply_queue'],{'reply_queue_id':('id','reply_queue_id'),'status':('status',),'author':('author','author_id'),'source_url':('source_url','url'),'license':('license',)})
    return build_reply_draft_source_license_exposure_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_reply_draft_source_license_exposure_json(r): return json_dumps(r)
def format_reply_draft_source_license_exposure_text(r):
    lines=['Reply Draft Source License Exposure',f"Generated: {r['generated_at']}",f"Totals: rows={r['totals']['rows']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines += ['','reply_queue_id | status | author | license | reason']
    for f in r['findings']: lines.append(f"{f['reply_queue_id']} | {f['status']} | {f['author']} | {f['license']} | {f['reason']}")
    return '\n'.join(lines)
