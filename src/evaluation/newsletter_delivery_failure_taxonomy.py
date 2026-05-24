"""Classify newsletter delivery failures into deterministic buckets."""
from __future__ import annotations
from collections import Counter,defaultdict
from typing import Any
from ._report_utils import clean,connection,expr,json_dumps,lower,now_iso,positive,schema
ARTIFACT_TYPE="newsletter_delivery_failure_taxonomy"; DEFAULT_LIMIT=50
def build_newsletter_delivery_failure_taxonomy_report(rows:list[dict[str,Any]],*,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("limit",limit); failures=[_norm(r) for r in rows if _is_failure(r)]
 totals=Counter(f["category"] for f in failures); providers=defaultdict(Counter); issues=defaultdict(Counter)
 for f in failures: providers[f["provider"]][f["category"]]+=1; issues[f["issue_id"]][f["category"]]+=1
 findings=sorted(failures,key=lambda f:(_rank(f["category"]),f["occurred_at"] or "",f["provider"],f["issue_id"],f["subscriber_id"]),reverse=True)[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"limit":limit},"totals":{"failure_count":len(failures),"by_category":dict(sorted(totals.items())),"shown_findings":len(findings)},"provider_breakdown":[{"provider":p,"total":sum(c.values()),"by_category":dict(sorted(c.items()))} for p,c in sorted(providers.items())],"issue_breakdown":[{"issue_id":i,"total":sum(c.values()),"by_category":dict(sorted(c.items()))} for i,c in sorted(issues.items())],"findings":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not failures,"message":"No newsletter delivery failures found." if not failures else None}}
def build_newsletter_delivery_failure_taxonomy_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn)
 if "newsletter_delivery_events" not in s: return build_newsletter_delivery_failure_taxonomy_report([],missing_tables=["newsletter_delivery_events"],**kwargs)
 mc={}; return build_newsletter_delivery_failure_taxonomy_report(_load(conn,s["newsletter_delivery_events"],mc),missing_columns=mc,**kwargs)
def format_newsletter_delivery_failure_taxonomy_json(report:dict[str,Any])->str: return json_dumps(report)
def format_newsletter_delivery_failure_taxonomy_text(report:dict[str,Any])->str:
 lines=["Newsletter Delivery Failure Taxonomy",f"Generated: {report['generated_at']}",f"Totals: failures={report['totals']['failure_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","category | provider | issue_id | subscriber_id | occurred_at | snippet"]
 for f in report["findings"]: lines.append(f"{f['category']} | {f['provider']} | {f['issue_id']} | {f['subscriber_id']} | {f['occurred_at'] or '-'} | {f['snippet']}")
 return "\n".join(lines)
def _load(conn:Any,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 required={"status"}; miss=sorted(c for c in required if c not in cols)
 if miss: mc["newsletter_delivery_events"]=miss
 select=[expr(cols,"status",default="'unknown'",out="status"),expr(cols,"error_code",out="error_code"),expr(cols,"error_message","provider_error","payload",out="error_message"),expr(cols,"provider",default="'unknown'",out="provider"),expr(cols,"issue_id","newsletter_issue_id",out="issue_id"),expr(cols,"subscriber_id","newsletter_subscriber_id",out="subscriber_id"),expr(cols,"occurred_at","created_at",out="occurred_at")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM newsletter_delivery_events ORDER BY rowid")]
def _is_failure(r:dict[str,Any])->bool:
 text=" ".join(lower(r.get(k)) for k in ("status","error_code","error_message"))
 return any(x in text for x in ("fail","bounce","reject","suppress","timeout","error","blocked"))
def _norm(r:dict[str,Any])->dict[str,Any]:
 text=" ".join(lower(r.get(k)) for k in ("status","error_code","error_message"))
 cat="suppression" if "suppress" in text or "unsubscribe" in text else "bounce" if "bounce" in text or "mailbox" in text else "provider_reject" if "reject" in text or "blocked" in text or "policy" in text else "timeout" if "timeout" in text or "timed out" in text else "unknown"
 return {"category":cat,"provider":clean(r.get("provider"),"unknown"),"issue_id":clean(r.get("issue_id"),"unknown"),"subscriber_id":clean(r.get("subscriber_id"),"unknown"),"occurred_at":clean(r.get("occurred_at")) or None,"snippet":clean(r.get("error_message") or r.get("error_code"))[:120]}
def _rank(c:str)->int: return {"provider_reject":4,"suppression":3,"bounce":2,"timeout":1}.get(c,0)
