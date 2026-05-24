"""Cluster visual asset render failures by normalized signature."""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import re
from typing import Any
from ._report_utils import clean, connection, expr, json_dumps, now_iso, positive, schema, dt
ARTIFACT_TYPE="visual_asset_render_failure_clusters"; DEFAULT_LIMIT=50; DEFAULT_LOOKBACK_DAYS=14; DEFAULT_MIN_CLUSTER_SIZE=2
def build_visual_asset_render_failure_clusters_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,min_cluster_size:int=DEFAULT_MIN_CLUSTER_SIZE,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:datetime|None=None)->dict[str,Any]:
    positive("lookback_days",lookback_days); positive("min_cluster_size",min_cluster_size); positive("limit",limit); gen=now or datetime.now(timezone.utc); cutoff=gen-timedelta(days=lookback_days); groups=defaultdict(list)
    for r in rows:
        t=dt(r.get("created_at"))
        if t and t<cutoff: continue
        if clean(r.get("status")).lower() not in {"failed","error","render_failed"} and not clean(r.get("error_message")): continue
        key=(clean(r.get("template"),"unknown"),clean(r.get("generator"),"unknown"),clean(r.get("dimensions"),"unknown"),_sig(clean(r.get("error_message"))))
        groups[key].append(r)
    findings=[{"template":k[0],"generator":k[1],"dimensions":k[2],"error_signature":k[3],"failure_count":len(v),"examples":[clean(x.get("asset_id")) for x in v[:5]]} for k,v in groups.items() if len(v)>=min_cluster_size]
    findings.sort(key=lambda r:(-r["failure_count"],r["template"],r["error_signature"])); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(gen),"filters":{"lookback_days":lookback_days,"limit":limit},"thresholds":{"min_cluster_size":min_cluster_size},"summary":{"failure_row_count":sum(len(v) for v in groups.values()),"cluster_count":len(groups),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())}}
def build_visual_asset_render_failure_clusters_report_from_db(db_or_conn:Any,**kwargs):
    conn=connection(db_or_conn); s=schema(conn); table="visual_assets"; mt=[] if table in s else [table]; mc={}; rows=[]
    if table in s:
        c=s[table]; req={"error_message"}
        if not req.issubset(c): mc[table]=sorted(req-c)
        else:
            select=[expr(c,"id","asset_id",default="rowid",out="asset_id"),expr(c,"template","template_id",default="NULL",out="template"),expr(c,"generator",default="NULL",out="generator"),expr(c,"dimensions",default="NULL",out="dimensions"),expr(c,"status",default="NULL",out="status"),"error_message",expr(c,"created_at","updated_at",default="NULL",out="created_at")]
            rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY rowid")]
    return build_visual_asset_render_failure_clusters_report(rows,missing_tables=mt,missing_columns=mc,**kwargs)
def format_visual_asset_render_failure_clusters_json(report): return json_dumps(report)
def format_visual_asset_render_failure_clusters_text(report):
    lines=["Visual Asset Render Failure Clusters",f"Generated: {report['generated_at']}",f"Totals: failures={report['summary']['failure_row_count']} findings={report['summary']['finding_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
    if not report["findings"]: lines.append("No visual asset render failure clusters found."); return "\n".join(lines)
    for r in report["findings"]: lines.append(f"- {r['template']} {r['generator']} {r['dimensions']} count={r['failure_count']} signature={r['error_signature']}")
    return "\n".join(lines)
def _sig(text:str)->str:
    text=re.sub(r"https?://\S+","<url>",text.lower())
    text=re.sub(r"(/[^\s]+)+","<path>",text)
    text=re.sub(r"\b[0-9a-f]{8,}\b","<id>",text)
    text=re.sub(r"\b\d{4}-\d{2}-\d{2}[t ][^\s]+\b","<time>",text)
    text=re.sub(r"\b\d+\b","<num>",text)
    return re.sub(r"\s+"," ",text).strip()
