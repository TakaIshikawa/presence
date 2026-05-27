"""Audit publish queue manual overrides for missing context and conflicts."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="publish_queue_manual_override_audit"; DEFAULT_MAX_AGE_HOURS=72; DEFAULT_STATUS="queued,scheduled,pending"; DEFAULT_LIMIT=100
def build_publish_queue_manual_override_audit_report(rows:list[dict[str,Any]],*,max_age_hours:int=DEFAULT_MAX_AGE_HOURS,status:str|None=DEFAULT_STATUS,limit:int=DEFAULT_LIMIT,now=None,missing_tables=None,missing_columns=None):
    positive("max_age_hours",max_age_hours); positive("limit",limit)
    gen=now_value(now); wanted={lower(p) for p in clean(status).split(",") if lower(p)}; findings=[]
    for r in rows:
        st=lower(r.get("status"),"queued")
        if wanted and st not in wanted: continue
        meta=_meta(r); override=_truthy(r.get("manual_override") or r.get("manual_approved") or meta.get("manual_override") or meta.get("manual_approved"))
        if not override: continue
        actor=clean(r.get("override_actor") or r.get("manual_actor") or meta.get("override_actor") or meta.get("actor"))
        reason=clean(r.get("override_reason") or r.get("manual_reason") or meta.get("override_reason") or meta.get("reason"))
        ts=dt(r.get("override_at") or r.get("manual_approved_at") or meta.get("override_at") or r.get("updated_at") or r.get("created_at")); age=round((gen-(ts or gen)).total_seconds()/3600,2)
        checks=[("missing_actor",not actor),("missing_reason",not reason),("stale_override",age>max_age_hours),("quality_gate_conflict",_failed_gate(r,meta))]
        for gap,ok in checks:
            if ok: findings.append({"queue_id":r.get("queue_id") or r.get("id"),"override_actor":actor or None,"override_reason":reason or None,"age_hours":age,"gap_reason":gap})
    findings.sort(key=lambda f:(_sid(f["queue_id"]),f["gap_reason"]))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"max_age_hours":max_age_hours,"status":status,"limit":limit},"summary":{"queue_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},"empty_state":empty_state(findings,"No publish queue manual override gaps found.",schema_gap=bool(missing_tables or missing_columns))}
def build_publish_queue_manual_override_audit_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; rows=[]
    if "publish_queue" not in s: mt.append("publish_queue")
    else:
        c=s["publish_queue"]
        if "id" not in c: mc["publish_queue"]=["id"]
        else: rows=load_table(conn,"publish_queue",c,{"queue_id":("id","queue_id"),"status":("status","state"),"manual_override":("manual_override","manual_approved"),"override_actor":("override_actor","manual_actor","approved_by"),"override_reason":("override_reason","manual_reason","approval_reason"),"override_at":("override_at","manual_approved_at","updated_at","created_at"),"quality_status":("quality_status","quality_gate_status"),"metadata":("metadata","quality_metadata")})
    return build_publish_queue_manual_override_audit_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def _meta(r):
    try:return json.loads(clean(r.get("metadata"))) if clean(r.get("metadata")) else {}
    except json.JSONDecodeError:return {}
def _truthy(v): return lower(v) in {"1","true","yes","y","approved","override"} or v is True
def _failed_gate(r,m): return lower(r.get("quality_status") or m.get("quality_status") or m.get("quality_gate_status")) in {"failed","fail","blocked","rejected"} or bool(m.get("failed_quality_gates"))
def format_publish_queue_manual_override_audit_json(r): return json_dumps(r)
def format_publish_queue_manual_override_audit_text(r):
    lines=["Publish Queue Manual Override Audit",f"Generated: {r['generated_at']}",f"Totals: queue={r['summary']['queue_count']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - queue={f['queue_id']} gap={f['gap_reason']} age={f['age_hours']}")
    return "\n".join(lines)
def _sid(v):
    try:return(0,int(v))
    except(TypeError,ValueError):return(1,clean(v))
