"""Audit manual publish queue overrides."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Any
from ._report_utils import clean, connection, expr, json_dumps, loads_obj, now_iso, positive, schema, dt
ARTIFACT_TYPE="publish_queue_manual_override_audit"; DEFAULT_LIMIT=50; DEFAULT_LOOKBACK_DAYS=30
def build_publish_queue_manual_override_audit_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:datetime|None=None)->dict[str,Any]:
    positive("lookback_days",lookback_days); positive("limit",limit); gen=now or datetime.now(timezone.utc); cutoff=gen-timedelta(days=lookback_days); findings=[]
    for row in rows:
        when=dt(row.get("override_at") or row.get("updated_at") or row.get("created_at"))
        if when and when<cutoff: continue
        meta=loads_obj(row.get("metadata")); override=clean(row.get("override_type") or meta.get("override_type") or meta.get("manual_override"))
        actor=clean(row.get("override_actor") or meta.get("actor") or meta.get("override_actor")); reason=clean(row.get("override_reason") or meta.get("reason") or meta.get("override_reason"))
        outcome=clean(row.get("publication_status") or row.get("published_at") or meta.get("publication_status") or meta.get("outcome"))
        if override or actor or reason:
            issues=[]
            if not actor: issues.append("missing_actor")
            if not reason: issues.append("missing_reason")
            if not outcome: issues.append("missing_publication_outcome")
            for issue in issues: findings.append({"queue_id":clean(row.get("queue_id")),"content_id":clean(row.get("content_id")) or None,"override_type":override or "manual","issue_type":issue,"actor":actor or None,"reason":reason or None,"publication_status":outcome or None})
    findings.sort(key=lambda r:(r["issue_type"],r["queue_id"])); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(gen),"filters":{"lookback_days":lookback_days,"limit":limit},"summary":{"row_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())}}
def build_publish_queue_manual_override_audit_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
    conn=connection(db_or_conn); s=schema(conn); table="publish_queue"; mt=[] if table in s else [table]; mc={}; rows=[]
    if table in s:
        c=s[table]; req={"id"}
        if not req.issubset(c): mc[table]=sorted(req-c)
        else:
            select=[expr(c,"id","queue_id",default="rowid",out="queue_id"),expr(c,"content_id",default="NULL",out="content_id"),expr(c,"override_type","manual_override_type",default="NULL",out="override_type"),expr(c,"override_actor","manual_override_actor",default="NULL",out="override_actor"),expr(c,"override_reason","manual_override_reason",default="NULL",out="override_reason"),expr(c,"publication_status","status",default="NULL",out="publication_status"),expr(c,"published_at",default="NULL",out="published_at"),expr(c,"override_at","updated_at",default="NULL",out="override_at"),expr(c,"created_at",default="NULL",out="created_at"),expr(c,"metadata",default="NULL",out="metadata")]
            rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY rowid")]
    return build_publish_queue_manual_override_audit_report(rows,missing_tables=mt,missing_columns=mc,**kwargs)
def format_publish_queue_manual_override_audit_json(report): return json_dumps(report)
def format_publish_queue_manual_override_audit_text(report):
    lines=["Publish Queue Manual Override Audit",f"Generated: {report['generated_at']}",f"Totals: rows={report['summary']['row_count']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
    if not report["findings"]: lines.append("No publish queue manual override audit findings found."); return "\n".join(lines)
    for r in report["findings"]: lines.append(f"- queue={r['queue_id']} type={r['override_type']} issue={r['issue_type']} actor={r['actor'] or '-'}")
    return "\n".join(lines)
