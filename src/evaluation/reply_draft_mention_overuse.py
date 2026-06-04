"""Identify reply drafts with excessive or repeated mentions."""
from __future__ import annotations
from collections import Counter
from typing import Any
import re
from ._batch_report_common import *

ARTIFACT_TYPE="reply_draft_mention_overuse"; DEFAULT_MAX_MENTIONS=3; DEFAULT_MAX_REPEATED_HANDLE_COUNT=1; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_LIMIT=50
_MENTION_RE=re.compile(r"(?<![A-Za-z0-9_.%+-])@([A-Za-z0-9_]{1,30})\b")

def build_reply_draft_mention_overuse_report(rows:list[dict[str,Any]],*,max_mentions:int=DEFAULT_MAX_MENTIONS,max_repeated_handle_count:int=DEFAULT_MAX_REPEATED_HANDLE_COUNT,lookback_days:int=DEFAULT_LOOKBACK_DAYS,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now=None)->dict[str,Any]:
    positive("max_mentions",max_mentions); positive("max_repeated_handle_count",max_repeated_handle_count); positive("lookback_days",lookback_days); positive("limit",limit)
    gen=now_value(now); cutoff=gen-timedelta(days=lookback_days); findings=[]; scanned=0
    for row in rows:
        if lower(row.get("type") or row.get("draft_type"),"reply")!="reply" or lower(row.get("status"),"draft") not in {"draft","ready","pending"}: continue
        created=dt(row.get("created_at") or row.get("updated_at"))
        if created and created<cutoff: continue
        scanned+=1; text=clean(row.get("body") or row.get("text") or row.get("content")); handles=[h.lower() for h in _MENTION_RE.findall(text)]
        counts=Counter(handles); repeated={h:c for h,c in sorted(counts.items()) if c>max_repeated_handle_count}
        if len(handles)>max_mentions or repeated:
            findings.append({"draft_id":clean(row.get("id") or row.get("draft_id")),"target_id":clean(row.get("target_id") or row.get("reply_to_id") or row.get("conversation_id")),"mention_count":len(handles),"repeated_handles":repeated,"draft_excerpt":text[:160],"created_at":created.isoformat() if created else "","_created_ts":created.timestamp() if created else 0.0})
    findings.sort(key=lambda i:(-i["mention_count"],-max(i["repeated_handles"].values(),default=0),-i["_created_ts"],i["draft_id"]))
    findings=findings[:limit]
    for item in findings: item.pop("_created_ts",None)
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"max_mentions":max_mentions,"max_repeated_handle_count":max_repeated_handle_count,"lookback_days":lookback_days,"limit":limit},"summary":{"drafts_scanned":scanned,"overuse_count":len(findings)},"overuse":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No reply draft mention overuse found.",schema_gap=bool(missing_tables or missing_columns))}

def build_reply_draft_mention_overuse_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
    conn=connection(db_or_conn); sch=schema(conn); table=next((t for t in ("reply_drafts","draft_replies") if t in sch),None)
    if not table: return build_reply_draft_mention_overuse_report([],missing_tables=["reply_drafts"],**kwargs)
    rows=load_table(conn,table,sch[table],{"id":("id","draft_id"),"target_id":("target_id","reply_to_id","conversation_id"),"type":("type","draft_type"),"status":("status",),"body":("body","text","content"),"created_at":("created_at","updated_at")})
    return build_reply_draft_mention_overuse_report(rows,**kwargs)
def format_reply_draft_mention_overuse_json(report:dict[str,Any])->str: return json_dumps(report)
def format_reply_draft_mention_overuse_text(report:dict[str,Any])->str:
    lines=["Reply Draft Mention Overuse",f"Generated: {report['generated_at']}",f"Totals: scanned={report['summary']['drafts_scanned']} overuse={report['summary']['overuse_count']}"]
    if not report["overuse"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","draft_id | target_id | mentions | repeated | excerpt"]+[f"{i['draft_id']} | {i['target_id']} | {i['mention_count']} | {i['repeated_handles']} | {i['draft_excerpt']}" for i in report["overuse"]]
    return "\n".join(lines)
