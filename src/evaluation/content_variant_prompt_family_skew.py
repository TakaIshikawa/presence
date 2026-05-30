"""Find content variant groups with too little prompt-family diversity."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._report_utils import clean, connection, expr, json_dumps, now_iso, positive, schema
ARTIFACT_TYPE="content_variant_prompt_family_skew"; DEFAULT_LIMIT=50; DEFAULT_MIN_FAMILY_COUNT=2
def build_content_variant_prompt_family_skew_report(rows:list[dict[str,Any]],*,min_family_count:int=DEFAULT_MIN_FAMILY_COUNT,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
    positive("min_family_count",min_family_count); positive("limit",limit); groups=defaultdict(list)
    for r in rows: groups[(clean(r.get("content_id")),clean(r.get("campaign_id")),clean(r.get("platform")))].append(r)
    findings=[]
    for (cid,camp,platform), items in groups.items():
        fam={clean(x.get("prompt_family")) for x in items if clean(x.get("prompt_family"))}; ver={clean(x.get("prompt_version")) for x in items if clean(x.get("prompt_version"))}
        if len(items)>1 and (len(fam)<min_family_count or len(ver)<min_family_count):
            findings.append({"content_id":cid,"campaign_id":camp or None,"platform":platform or None,"variant_count":len(items),"prompt_family_count":len(fam),"prompt_version_count":len(ver),"issue_type":"low_prompt_family_diversity" if len(fam)<min_family_count else "low_prompt_version_diversity"})
    findings.sort(key=lambda r:(r["prompt_family_count"],r["prompt_version_count"],r["content_id"],r["platform"] or "")); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"thresholds":{"min_family_count":min_family_count,"limit":limit},"summary":{"variant_count":len(rows),"group_count":len(groups),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())}}
def build_content_variant_prompt_family_skew_report_from_db(db_or_conn:Any,**kwargs):
    conn=connection(db_or_conn); s=schema(conn); table="content_variants"; mt=[] if table in s else [table]; mc={}; rows=[]
    if table in s:
        c=s[table]; req={"content_id"}
        if not req.issubset(c): mc[table]=sorted(req-c)
        else:
            select=["content_id",expr(c,"campaign_id",default="NULL",out="campaign_id"),expr(c,"platform",default="NULL",out="platform"),expr(c,"prompt_family",default="NULL",out="prompt_family"),expr(c,"prompt_version",default="NULL",out="prompt_version")]
            rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY rowid")]
    return build_content_variant_prompt_family_skew_report(rows,missing_tables=mt,missing_columns=mc,**kwargs)
def format_content_variant_prompt_family_skew_json(report): return json_dumps(report)
def format_content_variant_prompt_family_skew_text(report):
    lines=["Content Variant Prompt Family Skew",f"Generated: {report['generated_at']}",f"Totals: variants={report['summary']['variant_count']} findings={report['summary']['finding_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
    if not report["findings"]: lines.append("No content variant prompt family skew found."); return "\n".join(lines)
    for r in report["findings"]: lines.append(f"- content={r['content_id']} campaign={r['campaign_id'] or '-'} platform={r['platform'] or '-'} families={r['prompt_family_count']} versions={r['prompt_version_count']}")
    return "\n".join(lines)
