"""Report expected versus actual publication media attachment mismatches."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE="publication_media_attachment_mismatch"

def build_publication_media_attachment_mismatch_report(content_rows:list[dict[str,Any]],attempt_rows:list[dict[str,Any]],*,platform:str|None=None,since:str|None=None,missing_tables=None,missing_columns=None,now=None)->dict[str,Any]:
    pf=lower(platform); since_dt=dt(since); by_id={clean(r.get("content_id") or r.get("id")):r for r in content_rows}; findings=[]
    for row in attempt_rows:
        if pf and lower(row.get("platform"))!=pf: continue
        created=dt(row.get("created_at") or row.get("queued_at") or row.get("published_at"))
        if since_dt and created and created<since_dt: continue
        cid=clean(row.get("content_id")); content=by_id.get(cid,{})
        expected=_media_count(content.get("expected_media_count") or content.get("media_count") or content.get("metadata") or content.get("content_metadata"))
        actual=_media_count(row.get("actual_media_count") or row.get("media_count") or row.get("payload") or row.get("metadata"))
        if expected>actual: kind="missing_media"
        elif expected==0 and actual>0: kind="unexpected_media"
        else: continue
        findings.append({"mismatch_type":kind,"content_id":cid or None,"platform":clean(row.get("platform"),"unknown"),"attempt_id":row.get("attempt_id") or row.get("id"),"expected_media_count":expected,"actual_media_count":actual})
    findings.sort(key=lambda f:(f["mismatch_type"],str(f["platform"]),str(f["content_id"]),str(f["attempt_id"])))
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"platform":platform,"since":since},"totals":{"content_rows":len(content_rows),"attempt_rows":len(attempt_rows),"findings":len(findings)},"findings":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No publication media attachment mismatches found.",schema_gap=bool(missing_tables or missing_columns))}

def build_publication_media_attachment_mismatch_report_from_db(db_or_conn:Any,**kw)->dict[str,Any]:
    conn=connection(db_or_conn); s=schema(conn); mt=[]; content=[]; attempts=[]; ctable="generated_content" if "generated_content" in s else "content_items" if "content_items" in s else None
    if not ctable: mt.append("generated_content")
    else: content=load_table(conn,ctable,s[ctable],{"content_id":("id","content_id"),"metadata":("metadata","content_metadata"),"expected_media_count":("expected_media_count","media_count","attachment_count")})
    atable="publication_attempts" if "publication_attempts" in s else "publish_queue" if "publish_queue" in s else "content_publications" if "content_publications" in s else None
    if not atable: mt.append("publication_attempts")
    else: attempts=load_table(conn,atable,s[atable],{"attempt_id":("id","attempt_id"),"content_id":("content_id","generated_content_id"),"platform":("platform","provider"),"payload":("payload","request_payload","metadata"),"actual_media_count":("actual_media_count","media_count","attachment_count"),"created_at":("created_at","queued_at","published_at")})
    return build_publication_media_attachment_mismatch_report(content,attempts,missing_tables=mt,**kw)

def format_publication_media_attachment_mismatch_json(r): return json_dumps(r)
def format_publication_media_attachment_mismatch_text(r):
    lines=["Publication Media Attachment Mismatch",f"Generated: {r['generated_at']}",f"Totals: attempts={r['totals']['attempt_rows']} findings={r['totals']['findings']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","type | content_id | platform | attempt_id | expected | actual"]
    for f in r["findings"]: lines.append(f"{f['mismatch_type']} | {f['content_id']} | {f['platform']} | {f['attempt_id']} | {f['expected_media_count']} | {f['actual_media_count']}")
    return "\n".join(lines)
def _media_count(value:Any)->int:
    if isinstance(value,(int,float)): return max(0,int(value))
    data=json.loads(value) if isinstance(value,str) and value.strip().startswith(("{","[")) else value
    if isinstance(data,list): return len(data)
    if isinstance(data,dict):
        for key in ("media","attachments","images","image_urls"):
            if isinstance(data.get(key),list): return len(data[key])
        for key in ("expected_media_count","media_count","attachment_count"):
            if key in data: return to_int(data[key])
    return to_int(value)
