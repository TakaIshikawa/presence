"""Report stale or missing knowledge embeddings."""
from __future__ import annotations
from datetime import datetime,timezone
from typing import Any
from ._report_utils import clean,connection,dt,expr,json_dumps,now_iso,positive,schema
ARTIFACT_TYPE="knowledge_embedding_staleness"; DEFAULT_MAX_AGE_DAYS=30; DEFAULT_LIMIT=50
def build_knowledge_embedding_staleness_report(rows:list[dict[str,Any]],*,max_age_days:int=DEFAULT_MAX_AGE_DAYS,current_model:str|None=None,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None)->dict[str,Any]:
 positive("max_age_days",max_age_days); positive("limit",limit); ref=now or datetime.now(timezone.utc); findings=[]
 for r in rows:
  updated=dt(r.get("updated_at")); embedded=dt(r.get("embedded_at")); model=clean(r.get("embedding_model")) or None; reasons=[]
  if not embedded: reasons.append("missing_embedding")
  elif updated and embedded<updated: reasons.append("stale_embedding")
  if current_model and not model: reasons.append("model_missing")
  elif current_model and model!=current_model: reasons.append("model_outdated")
  if embedded and (ref.astimezone(timezone.utc)-embedded).days>max_age_days and "stale_embedding" not in reasons: reasons.append("stale_embedding")
  if reasons:
   days=(ref.astimezone(timezone.utc)-(embedded or updated or ref)).days
   findings.append({"knowledge_id":str(r.get("knowledge_id")),"source_type":clean(r.get("source_type")) or None,"source_url":clean(r.get("source_url")) or None,"updated_at":updated.isoformat() if updated else None,"embedded_at":embedded.isoformat() if embedded else None,"embedding_model":model,"staleness_days":days,"reason":",".join(reasons)})
 findings.sort(key=lambda x:(-x["staleness_days"],x["knowledge_id"]))
 shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"thresholds":{"max_age_days":max_age_days,"current_model":current_model,"limit":limit},"summary":{"knowledge_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())}}
def build_knowledge_embedding_staleness_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); table=next((t for t in ("knowledge","knowledge_sources","knowledge_records") if t in s),None); miss=[] if table else ["knowledge|knowledge_sources"]; mc={}
 rows=_load(conn,table,s,mc) if table else []
 return build_knowledge_embedding_staleness_report(rows,missing_tables=miss,missing_columns=mc,**kwargs)
def format_knowledge_embedding_staleness_json(report:dict[str,Any])->str: return json_dumps(report)
def format_knowledge_embedding_staleness_text(report:dict[str,Any])->str:
 lines=["Knowledge Embedding Staleness",f"Generated: {report['generated_at']}",f"Totals: knowledge={report['summary']['knowledge_count']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append("No knowledge embedding staleness found."); return "\n".join(lines)
 lines+=["","knowledge_id | source_type | source_url | updated_at | embedded_at | embedding_model | staleness_days | reason"]
 for f in report["findings"]: lines.append(f"{f['knowledge_id']} | {f['source_type'] or '-'} | {f['source_url'] or '-'} | {f['updated_at'] or '-'} | {f['embedded_at'] or '-'} | {f['embedding_model'] or '-'} | {f['staleness_days']} | {f['reason']}")
 return "\n".join(lines)
def _load(conn:Any,table:str,s:dict[str,set[str]],mc:dict[str,list[str]])->list[dict[str,Any]]:
 kc=s[table]; kid=next((c for c in ("id","knowledge_id") if c in kc),None)
 if not kid: mc[table]=["id"]; return []
 ec=s.get("knowledge_embeddings",set()); join=""; embedded=expr(kc,"embedded_at","embedding_updated_at",default="NULL",alias="k",out="embedded_at"); model=expr(kc,"embedding_model",default="NULL",alias="k",out="embedding_model")
 if "knowledge_embeddings" in s and "knowledge_id" in ec:
  join=f"LEFT JOIN knowledge_embeddings e ON e.knowledge_id = k.{kid}"; embedded=expr(ec,"embedded_at","created_at","updated_at",default="NULL",alias="e",out="embedded_at"); model=expr(ec,"embedding_model","model",default="NULL",alias="e",out="embedding_model")
 select=[f"k.{kid} AS knowledge_id",expr(kc,"source_type","type",default="NULL",alias="k",out="source_type"),expr(kc,"source_url","url",default="NULL",alias="k",out="source_url"),expr(kc,"updated_at","modified_at",default="NULL",alias="k",out="updated_at"),embedded,model]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} k {join} ORDER BY k.rowid")]
