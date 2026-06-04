"""Find unconverted content ideas whose evidence is stale."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="content_idea_recency_decay"; DEFAULT_STALE_AFTER_DAYS=60; DEFAULT_WARNING_AFTER_DAYS=30; DEFAULT_LIMIT=50; DEFAULT_STATUSES=("new","open","planned","draft")
def build_content_idea_recency_decay_report(rows:list[dict[str,Any]],*,stale_after_days:int=DEFAULT_STALE_AFTER_DAYS,warning_after_days:int=DEFAULT_WARNING_AFTER_DAYS,statuses:tuple[str,...]|list[str]=DEFAULT_STATUSES,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now=None)->dict[str,Any]:
 positive("stale_after_days",stale_after_days); positive("warning_after_days",warning_after_days); positive("limit",limit)
 gen=now_value(now); allowed={lower(s) for s in statuses}; findings=[]; scanned=0
 for row in rows:
  status=lower(row.get("status"),"open")
  if allowed and status not in allowed: continue
  scanned+=1; newest=dt(row.get("newest_evidence_at") or row.get("evidence_at") or row.get("updated_at")); count=to_int(row.get("evidence_count"),0)
  age=(gen-newest).days if newest else None
  if newest is None or age>=stale_after_days: severity="stale"
  elif age>=warning_after_days: severity="warning"
  else: continue
  reason="no evidence linked" if newest is None or count<=0 else f"newest evidence is {age} days old"
  findings.append({"idea_id":clean(row.get("id") or row.get("idea_id")),"title":clean(row.get("title")),"status":status,"newest_evidence_at":newest.isoformat() if newest else None,"age_days":age,"severity":severity,"evidence_count":count,"reason":reason})
 findings.sort(key=lambda i:({"stale":0,"warning":1}[i["severity"]],-(i["age_days"] if i["age_days"] is not None else 10**9),i["idea_id"]))
 findings=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"stale_after_days":stale_after_days,"warning_after_days":warning_after_days,"statuses":list(statuses),"limit":limit},"summary":{"ideas_scanned":scanned,"decay_count":len(findings)},"decays":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No content idea recency decay found.",schema_gap=bool(missing_tables or missing_columns))}
def build_content_idea_recency_decay_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); sch=schema(conn)
 if "content_ideas" not in sch: return build_content_idea_recency_decay_report([],missing_tables=["content_ideas"],**kwargs)
 cols=sch["content_ideas"]; rows=load_table(conn,"content_ideas",cols,{"id":("id","idea_id"),"title":("title",), "status":("status",), "newest_evidence_at":("newest_evidence_at","evidence_at","updated_at"),"evidence_count":("evidence_count",)})
 return build_content_idea_recency_decay_report(rows,**kwargs)
def format_content_idea_recency_decay_json(report:dict[str,Any])->str: return json_dumps(report)
def format_content_idea_recency_decay_text(report:dict[str,Any])->str:
 lines=["Content Idea Recency Decay",f"Generated: {report['generated_at']}",f"Totals: scanned={report['summary']['ideas_scanned']} decay={report['summary']['decay_count']}"]
 if not report["decays"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
 lines+=["","idea_id | title | status | age_days | severity | evidence_count"]+[f"{i['idea_id']} | {i['title']} | {i['status']} | {i['age_days']} | {i['severity']} | {i['evidence_count']}" for i in report["decays"]]
 return "\n".join(lines)
