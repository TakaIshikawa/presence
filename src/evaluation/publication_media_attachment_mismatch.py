"""Find generated publication media expectation mismatches."""
from __future__ import annotations
import json
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE='publication_media_attachment_mismatch'; DEFAULT_LIMIT=50
def _json(v):
    if isinstance(v,(dict,list)): return v
    try: return json.loads(clean(v) or '{}')
    except json.JSONDecodeError: return {}
def _count(v):
    data=_json(v)
    if isinstance(data,list): return len(data)
    if isinstance(data,dict):
        for k in ('media','attachments','images','media_urls','asset_ids'):
            x=data.get(k)
            if isinstance(x,list): return len(x)
            if isinstance(x,str) and x.strip(): return 1
        for k in ('media_count','expected_media_count','attachment_count'):
            if k in data: return to_int(data.get(k),0)
    return to_int(v,0)
def _expected(row):
    meta=_json(row.get('metadata') or row.get('content_metadata') or row.get('generated_metadata'))
    for k in ('expected_media_count','media_count','attachment_count'):
        if k in row and row.get(k) is not None: return to_int(row.get(k),0)
        if isinstance(meta,dict) and k in meta: return to_int(meta.get(k),0)
    if isinstance(meta,dict):
        if meta.get('requires_media') or meta.get('has_media'): return max(1,_count(meta))
        if meta.get('text_only') is True: return 0
    return _count(meta)
def build_publication_media_attachment_mismatch_report(rows:list[dict[str,Any]],*,platform:str|None=None,since:str|None=None,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive('limit',limit); since_dt=dt(since); findings=[]
    for r in rows:
        p=clean(r.get('platform'),'unknown')
        t=dt(r.get('attempted_at') or r.get('queued_at') or r.get('published_at') or r.get('created_at'))
        if platform and p!=platform: continue
        if since_dt and t and t<since_dt: continue
        exp=_expected(r); act=_count(r.get('payload') or r.get('payload_json') or r.get('published_payload') or r.get('request_payload') or r.get('media') or r.get('attachments'))
        typ='missing_media' if exp>0 and act<exp else ('unexpected_media' if exp==0 and act>0 else None)
        if typ: findings.append({'mismatch_type':typ,'content_id':r.get('content_id'),'platform':p,'attempt_id':r.get('attempt_id') or r.get('queue_id') or r.get('id'),'expected_media_count':exp,'actual_media_count':act,'observed_at':t.isoformat() if t else None,'source':r.get('source','publication_attempts'),'severity':abs(exp-act)})
    findings.sort(key=lambda f:(-f['severity'], f.get('observed_at') or '', str(f.get('content_id')), str(f.get('attempt_id'))))
    return {'artifact_type':ARTIFACT_TYPE,'generated_at':now_iso(now),'filters':{'platform':platform,'since':since,'limit':limit},'totals':{'rows':len(rows),'findings':len(findings)},'findings':findings[:limit],'missing_tables':sorted(missing_tables or []),'missing_columns':{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},'empty_state':empty_state(findings,'No publication media attachment mismatches found.',schema_gap=bool(missing_tables or missing_columns))}
def build_publication_media_attachment_mismatch_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; rows=[]
    if 'generated_content' not in s: mt.append('generated_content')
    for table,source in (('publication_attempts','publication_attempts'),('publish_queue','publish_queue'),('content_publications','content_publications')):
        if table not in s or 'generated_content' not in s: continue
        cols=s[table]; gc=s['generated_content']; cid='content_id' if 'content_id' in cols else ('generated_content_id' if 'generated_content_id' in cols else None)
        if not cid: continue
        def pc(*names, default='NULL', out=None):
            for name in names:
                if name in cols: return f"p.{name} AS {out or name}"
            return f"{default} AS {out or names[0]}"
        def gcc(*names, default='NULL', out=None):
            for name in names:
                if name in gc: return f"gc.{name} AS {out or name}"
            return f"{default} AS {out or names[0]}"
        select=[f"p.{cid} AS content_id", pc('id','attempt_id','queue_id',out='attempt_id'), pc('platform',default="'unknown'",out='platform'), pc('payload','payload_json','published_payload','request_payload','media','attachments',out='payload'), pc('attempted_at','queued_at','published_at','created_at','updated_at',out='attempted_at'), gcc('metadata','content_metadata','generated_metadata',out='metadata'), gcc('expected_media_count','media_count','attachment_count',out='expected_media_count'), f"'{source}' AS source"]
        rows += [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} p LEFT JOIN generated_content gc ON gc.id=p.{cid}")]
    if not any(t in s for t in ('publication_attempts','publish_queue','content_publications')): mt.append('publication_attempts')
    return build_publication_media_attachment_mismatch_report(rows,missing_tables=mt,missing_columns={},**kw)
def format_publication_media_attachment_mismatch_json(r): return json_dumps(r)
def format_publication_media_attachment_mismatch_text(r):
    lines=['Publication Media Attachment Mismatch',f"Generated: {r['generated_at']}",f"Totals: rows={r['totals']['rows']} findings={r['totals']['findings']}"]
    if r['missing_tables']: lines.append('Missing tables: '+', '.join(r['missing_tables']))
    if not r['findings']: lines.append(r['empty_state']['message']); return '\n'.join(lines)
    lines+=['','type | content_id | platform | attempt_id | expected | actual']
    for f in r['findings']: lines.append(f"{f['mismatch_type']} | {f['content_id']} | {f['platform']} | {f['attempt_id']} | {f['expected_media_count']} | {f['actual_media_count']}")
    return '\n'.join(lines)
