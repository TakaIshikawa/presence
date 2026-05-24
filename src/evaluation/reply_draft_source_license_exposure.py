"""Flag reply drafts that cite restricted or missing-license sources."""
from __future__ import annotations
from typing import Any
from ._report_utils import clean,connection,expr,json_dumps,lower,now_iso,positive,schema
ARTIFACT_TYPE="reply_draft_source_license_exposure"; DEFAULT_LIMIT=50; RESTRICTED=("restricted","noncommercial","non-commercial","private","proprietary","unknown","")
def build_reply_draft_source_license_exposure_report(rows:list[dict[str,Any]],*,include_posted:bool=False,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("limit",limit); findings=[]
 for r in rows:
  status=lower(r.get("status"),"draft")
  if status=="posted" and not include_posted: continue
  lic=lower(r.get("license"),"")
  reason="missing_license" if not lic else next((x for x in RESTRICTED if x and x in lic),None)
  if reason: findings.append({"reply_queue_id":clean(r.get("reply_queue_id"),"unknown"),"status":status,"author":clean(r.get("author"),"unknown"),"source_url":clean(r.get("source_url")) or None,"license":clean(r.get("license")) or None,"reason":reason})
 findings.sort(key=lambda f:(f["reason"],f["reply_queue_id"],f["source_url"] or "")); shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"include_posted":include_posted,"limit":limit},"totals":{"row_count":len(rows),"finding_count":len(findings),"shown_findings":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No reply draft source license exposure found." if not findings else None}}
def build_reply_draft_source_license_exposure_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); required=[t for t in ("reply_queue","reply_knowledge_links","knowledge") if t not in s]
 if required: return build_reply_draft_source_license_exposure_report([],missing_tables=required,**kwargs)
 return build_reply_draft_source_license_exposure_report(_load(conn,s),**kwargs)
def format_reply_draft_source_license_exposure_json(report:dict[str,Any])->str: return json_dumps(report)
def format_reply_draft_source_license_exposure_text(report:dict[str,Any])->str:
 lines=["Reply Draft Source License Exposure",f"Generated: {report['generated_at']}",f"Totals: rows={report['totals']['row_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","reply_queue_id | status | author | license | reason | source_url"]
 for f in report["findings"]: lines.append(f"{f['reply_queue_id']} | {f['status']} | {f['author']} | {f['license'] or '-'} | {f['reason']} | {f['source_url'] or '-'}")
 return "\n".join(lines)
def _load(conn:Any,s:dict[str,set[str]])->list[dict[str,Any]]:
 rq,rl,k=s["reply_queue"],s["reply_knowledge_links"],s["knowledge"]
 join="rk.reply_queue_id=r.id" if "reply_queue_id" in rl and "id" in rq else "1=0"
 kjoin="k.id=rk.knowledge_id" if "knowledge_id" in rl and "id" in k else "1=0"
 select=[expr(rq,"id",alias="r",out="reply_queue_id"),expr(rq,"status",default="'draft'",alias="r",out="status"),expr(rq,"author","author_id",alias="r",out="author"),expr(k,"url","source_url",alias="k",out="source_url"),expr(k,"license","license_name",alias="k",out="license")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM reply_queue r JOIN reply_knowledge_links rk ON {join} JOIN knowledge k ON {kjoin} ORDER BY r.rowid")]
