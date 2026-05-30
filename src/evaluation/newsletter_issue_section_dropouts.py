"""Compare planned and rendered newsletter issue sections."""
from __future__ import annotations
from collections import Counter
from typing import Any
from ._report_utils import clean, connection, expr, json_dumps, loads_list, loads_obj, now_iso, positive, schema
ARTIFACT_TYPE="newsletter_issue_section_dropouts"; DEFAULT_LIMIT=50; DEFAULT_MIN_REPEAT_COUNT=1
def build_newsletter_issue_section_dropouts_report(rows:list[dict[str,Any]],*,min_repeat_count:int=DEFAULT_MIN_REPEAT_COUNT,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
    positive("min_repeat_count",min_repeat_count); positive("limit",limit); findings=[]; missing_counter=Counter()
    for r in rows:
        planned=_sections(r.get("planned_sections"),r.get("metadata"),"planned_sections"); rendered=_sections(r.get("rendered_sections"),r.get("metadata"),"rendered_sections")
        for sec in planned:
            if sec not in rendered or not clean(rendered.get(sec)):
                missing_counter[sec]+=1; findings.append({"issue_id":clean(r.get("issue_id")),"section":sec,"issue_type":"missing_section" if sec not in rendered else "empty_section","repeat_count":missing_counter[sec]})
    findings=[f for f in findings if f["repeat_count"]>=min_repeat_count]; findings.sort(key=lambda r:(-r["repeat_count"],r["section"],r["issue_id"])); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"thresholds":{"min_repeat_count":min_repeat_count,"limit":limit},"summary":{"issue_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())}}
def build_newsletter_issue_section_dropouts_report_from_db(db_or_conn:Any,**kwargs):
    conn=connection(db_or_conn); s=schema(conn); table="newsletter_issues"; mt=[] if table in s else [table]; mc={}; rows=[]
    if table in s:
        c=s[table]; req={"id"}
        if not req.issubset(c): mc[table]=sorted(req-c)
        else:
            select=[expr(c,"id","issue_id",default="rowid",out="issue_id"),expr(c,"planned_sections",default="NULL",out="planned_sections"),expr(c,"rendered_sections","sent_sections",default="NULL",out="rendered_sections"),expr(c,"metadata",default="NULL",out="metadata")]
            rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY rowid")]
    return build_newsletter_issue_section_dropouts_report(rows,missing_tables=mt,missing_columns=mc,**kwargs)
def format_newsletter_issue_section_dropouts_json(report): return json_dumps(report)
def format_newsletter_issue_section_dropouts_text(report):
    lines=["Newsletter Issue Section Dropouts",f"Generated: {report['generated_at']}",f"Totals: issues={report['summary']['issue_count']} findings={report['summary']['finding_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
    if not report["findings"]: lines.append("No newsletter issue section dropouts found."); return "\n".join(lines)
    for r in report["findings"]: lines.append(f"- issue={r['issue_id']} section={r['section']} type={r['issue_type']} repeats={r['repeat_count']}")
    return "\n".join(lines)
def _sections(value,metadata,key):
    raw=value or loads_obj(metadata).get(key) or []
    if isinstance(raw,str): raw=loads_list(raw) or [x.strip() for x in raw.split(",") if x.strip()]
    if isinstance(raw,list): return {clean(x): clean(x) for x in raw if clean(x)}
    if isinstance(raw,dict): return {clean(k): clean(v) for k,v in raw.items() if clean(k)}
    return {}
